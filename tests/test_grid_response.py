from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from grid_response.acceptance import run as acceptance_run
from grid_response.api import JsonApplication
from grid_response.clock import FrozenClock
from grid_response.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from grid_response.planning import (
    PhaseInput,
    ResourceInput,
    TenantFloorInput,
    generate_candidates,
    ramp_limited_average,
    recovery_minutes_needed,
)
from grid_response.service import GridResponseService


ROOT = Path(__file__).resolve().parents[1]


def _phase(seq: int, start: str, end: str, target: str, is_final: bool = False) -> PhaseInput:
    from grid_response.clock import parse_utc

    return PhaseInput(
        phase_seq=seq,
        starts_at=parse_utc(start),
        ends_at=parse_utc(end),
        target_reduction_kw=Decimal(target),
        is_final=is_final,
    )


class PlanningTests(unittest.TestCase):
    def test_ramp_limited_average_partial_and_full(self) -> None:
        partial = ramp_limited_average(Decimal("2000"), Decimal("100"), Decimal("10"))
        self.assertEqual(partial, Decimal("500"))
        full = ramp_limited_average(Decimal("2000"), Decimal("100"), Decimal("60"))
        self.assertEqual(full, Decimal("2000") * (1 - Decimal(20) / Decimal(120)))

    def test_protected_resources_are_never_selected(self) -> None:
        phases = [_phase(0, "2026-09-26T14:00:00Z", "2026-09-26T15:00:00Z", "100")]
        protected = ResourceInput("train-a", "job_delay", "tenant-a", Decimal("2000"), Decimal("50"), 60, None, True)
        candidates = generate_candidates(
            phases=phases,
            resources=[protected],
            floors=[TenantFloorInput("tenant-a", Decimal("5000"), Decimal("3000"))],
            recovery_minutes=30,
        )
        for candidate in candidates:
            self.assertEqual(candidate["actions"], [])
            self.assertEqual(candidate["total_shortfall_kw"], "100.000")

    def test_tenant_floor_caps_reduction(self) -> None:
        phases = [_phase(0, "2026-09-26T14:00:00Z", "2026-09-26T15:00:00Z", "5000")]
        resource = ResourceInput("cap-a", "power_cap", "tenant-a", Decimal("3000"), Decimal("100"), None, None, False)
        candidates = generate_candidates(
            phases=phases,
            resources=[resource],
            floors=[TenantFloorInput("tenant-a", Decimal("5000"), Decimal("4200"))],
            recovery_minutes=30,
        )
        for candidate in candidates:
            total = sum(Decimal(action["planned_kw"]) for action in candidate["actions"])
            self.assertLessEqual(total, Decimal("800.000"))

    def test_storage_energy_budget_spans_phases(self) -> None:
        phases = [
            _phase(0, "2026-09-26T14:00:00Z", "2026-09-26T15:00:00Z", "900"),
            _phase(1, "2026-09-26T15:00:00Z", "2026-09-26T16:00:00Z", "900", is_final=True),
        ]
        storage = ResourceInput("storage-1", "storage", None, Decimal("1000"), Decimal("1000"), None, Decimal("1000"), False)
        candidates = generate_candidates(phases=phases, resources=[storage], floors=[], recovery_minutes=30)
        for candidate in candidates:
            by_phase: dict[int, Decimal] = {}
            for action in candidate["actions"]:
                by_phase[action["phase_seq"]] = by_phase.get(action["phase_seq"], Decimal(0)) + Decimal(action["planned_kw"])
            self.assertEqual(by_phase[0], Decimal("900.000"))
            # 储能在第一阶段消耗 900 kWh，第二阶段只剩 100 kWh 可放。
            self.assertEqual(by_phase[1], Decimal("100.000"))

    def test_final_phase_recovery_window_caps_output(self) -> None:
        phases = [_phase(0, "2026-09-26T15:00:00Z", "2026-09-26T16:00:00Z", "5000", is_final=True)]
        resource = ResourceInput("cap-a", "power_cap", "tenant-a", Decimal("3000"), Decimal("10"), None, None, False)
        candidates = generate_candidates(
            phases=phases,
            resources=[resource],
            floors=[TenantFloorInput("tenant-a", Decimal("9000"), Decimal("1000"))],
            recovery_minutes=30,
        )
        for candidate in candidates:
            total = sum(Decimal(action["planned_kw"]) for action in candidate["actions"])
            # 恢复窗口 30 分钟 × 爬坡 10 kW/分钟 = 300 kW 上限。
            self.assertEqual(total, Decimal("300.000"))

    def test_candidates_are_deterministic_and_ranked(self) -> None:
        phases = [_phase(0, "2026-09-26T14:00:00Z", "2026-09-26T15:00:00Z", "1000")]
        resources = [
            ResourceInput("storage-1", "storage", None, Decimal("2000"), Decimal("100"), None, Decimal("6000"), False),
            ResourceInput("cap-a", "power_cap", "tenant-a", Decimal("1200"), Decimal("20"), None, None, False),
        ]
        floors = [TenantFloorInput("tenant-a", Decimal("5000"), Decimal("3000"))]
        first = generate_candidates(phases=phases, resources=resources, floors=floors, recovery_minutes=30)
        second = generate_candidates(phases=phases, resources=resources, floors=floors, recovery_minutes=30)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 3)
        self.assertEqual({candidate["strategy"] for candidate in first}, {"storage_first", "cap_first", "job_first"})
        shortfalls = [Decimal(candidate["total_shortfall_kw"]) for candidate in first]
        self.assertEqual(shortfalls, sorted(shortfalls))
        self.assertEqual(first[0]["total_shortfall_kw"], "0.000")

    def test_recovery_minutes_needed_rounds_up(self) -> None:
        self.assertEqual(recovery_minutes_needed(Decimal("1666.666"), Decimal("100")), 17)
        self.assertEqual(recovery_minutes_needed(Decimal("0"), Decimal("100")), 0)


class GridServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 13, 0, tzinfo=timezone.utc))
        self.service = GridResponseService(self.connection, self.clock)
        for user_id, role in (("plan", "planner"), ("ops", "operator"), ("disp", "dispatcher"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)
        self.service.register_site("plan", {"site_id": "site-1", "name": "算力园区", "timezone": "Asia/Shanghai", "baseline_kw": "12000"})
        self.service.register_tenant_floor("plan", "site-1", {"tenant_id": "tenant-a", "current_load_kw": "5000", "min_capacity_kw": "3000"})
        self.service.register_tenant_floor("plan", "site-1", {"tenant_id": "tenant-b", "current_load_kw": "4000", "min_capacity_kw": "2000"})
        self.service.register_resource("plan", "site-1", {"resource_id": "storage-1", "kind": "storage", "adjustable_kw": "2000", "ramp_kw_per_minute": "100", "energy_kwh": "6000"})
        self.service.register_resource("plan", "site-1", {"resource_id": "cap-a", "kind": "power_cap", "tenant_id": "tenant-a", "adjustable_kw": "1200", "ramp_kw_per_minute": "20"})
        self.service.register_resource("plan", "site-1", {"resource_id": "delay-b", "kind": "job_delay", "tenant_id": "tenant-b", "adjustable_kw": "800", "ramp_kw_per_minute": "40", "max_delay_minutes": 60})
        self.service.register_resource("plan", "site-1", {"resource_id": "train-a", "kind": "job_delay", "tenant_id": "tenant-a", "adjustable_kw": "2000", "ramp_kw_per_minute": "50", "max_delay_minutes": 30, "protected": True})

    def tearDown(self) -> None:
        self.connection.close()

    def _event(self, event_id: str = "DR-1", curve: list[dict[str, str]] | None = None) -> dict[str, object]:
        curve = curve or [
            {"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "2500"},
            {"starts_at": "2026-09-26T15:00:00Z", "ends_at": "2026-09-26T16:00:00Z", "target_reduction_kw": "2800"},
        ]
        return self.service.register_event("plan", {
            "event_id": event_id,
            "site_id": "site-1",
            "window_start": "2026-09-26T14:00:00Z",
            "window_end": "2026-09-26T16:00:00Z",
            "recovery_minutes": 30,
            "curve": curve,
        })

    def _confirm(self, event_id: str = "DR-1", strategy: str = "storage_first", version: int = 1) -> dict[str, object]:
        run = self.service.generate_candidates("plan", event_id)
        candidate = next(item for item in run["candidates"] if item["strategy"] == strategy)
        return self.service.confirm_candidate("ops", event_id, candidate["candidate_id"], version)

    def _actions(self, event_id: str = "DR-1") -> list[dict[str, object]]:
        return self.service.event_detail(event_id)["actions"]

    def test_register_event_validates_curve_window_and_duplicates(self) -> None:
        with self.assertRaises(ValidationFailed):
            self._event(curve=[{"starts_at": "2026-09-26T13:00:00Z", "ends_at": "2026-09-26T14:00:00Z", "target_reduction_kw": "100"}])
        with self.assertRaises(ValidationFailed):
            self._event(curve=[
                {"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "100"},
                {"starts_at": "2026-09-26T14:30:00Z", "ends_at": "2026-09-26T15:30:00Z", "target_reduction_kw": "100"},
            ])
        with self.assertRaises(ValidationFailed):
            self.service.register_event("plan", {
                "event_id": "DR-past",
                "site_id": "site-1",
                "window_start": "2026-09-26T10:00:00Z",
                "window_end": "2026-09-26T12:00:00Z",
                "recovery_minutes": 30,
                "curve": [{"starts_at": "2026-09-26T10:00:00Z", "ends_at": "2026-09-26T12:00:00Z", "target_reduction_kw": "100"}],
            })
        self._event()
        with self.assertRaises(Conflict):
            self._event()

    def test_baseline_update_uses_optimistic_concurrency(self) -> None:
        updated = self.service.update_baseline("plan", "site-1", {"baseline_kw": "13000", "expected_revision": 1})
        self.assertEqual(updated["baseline_kw"], "13000")
        with self.assertRaises(Conflict):
            self.service.update_baseline("plan", "site-1", {"baseline_kw": "14000", "expected_revision": 1})

    def test_generate_candidates_replays_identical_input(self) -> None:
        self._event()
        first = self.service.generate_candidates("plan", "DR-1")
        second = self.service.generate_candidates("plan", "DR-1")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["run_id"], second["run_id"])
        self.assertEqual([c["candidate_id"] for c in first["candidates"]], [c["candidate_id"] for c in second["candidates"]])
        storage_first = next(item for item in first["candidates"] if item["strategy"] == "storage_first")
        self.assertTrue(storage_first["feasible"])
        resource_ids = {action["resource_id"] for action in storage_first["actions"]}
        self.assertNotIn("train-a", resource_ids)

    def test_confirm_freezes_resources_and_blocks_other_events(self) -> None:
        self._event()
        confirmed = self._confirm()
        self.assertEqual(confirmed["state"], "confirmed")
        self.assertEqual(sorted(confirmed["frozen_resources"]), ["cap-a", "delay-b", "storage-1"])
        for resource_id in ("cap-a", "delay-b", "storage-1"):
            resource = self.service.resource_detail(resource_id)
            self.assertEqual(resource["state"], "frozen")
            self.assertEqual(resource["frozen_event_id"], "DR-1")
        self.assertEqual(self.service.resource_detail("train-a")["state"], "available")
        with self.assertRaises(InvalidState):
            self.service.generate_candidates("plan", "DR-1")

    def test_confirm_conflict_rolls_back_all_freezes(self) -> None:
        self._event("DR-1", curve=[{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}])
        self._event("DR-2")
        dr2_run = self.service.generate_candidates("plan", "DR-2")
        storage_first = next(item for item in dr2_run["candidates"] if item["strategy"] == "storage_first")
        # DR-1 先确认并冻结 storage-1；DR-2 候选生成于冻结之前，确认时必须整体失败。
        self._confirm("DR-1")
        with self.assertRaises(Conflict):
            self.service.confirm_candidate("ops", "DR-2", storage_first["candidate_id"], 1)
        # 冲突必须整体回滚：DR-2 没有冻结任何资源，也没有留下动作。
        self.assertEqual(self.service.resource_detail("cap-a")["state"], "available")
        self.assertEqual(self.service.resource_detail("delay-b")["state"], "available")
        self.assertEqual(self._actions("DR-2"), [])
        detail = self.service.event_detail("DR-2")
        self.assertEqual(detail["state"], "planned")
        candidate_states = {candidate["candidate_id"]: candidate["state"] for candidate in detail["candidates"]}
        self.assertEqual(candidate_states[storage_first["candidate_id"]], "proposed")

    def test_confirm_rejects_stale_version(self) -> None:
        self._event()
        run = self.service.generate_candidates("plan", "DR-1")
        candidate = run["candidates"][0]
        with self.assertRaises(InvalidState):
            self.service.confirm_candidate("ops", "DR-1", candidate["candidate_id"], 2)

    def test_receipts_merge_by_event_time_and_protect_terminal(self) -> None:
        self._event(curve=[{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}])
        self._confirm()
        action_id = self._actions()[0]["action_id"]
        self.clock.advance(minutes=70)  # 14:10

        def receipt(receipt_id: str, status: str, measured: str, occurred: str) -> dict[str, object]:
            return self.service.record_receipt("disp", "DR-1", {
                "receipt_id": receipt_id,
                "action_id": action_id,
                "status": status,
                "measured_kw": measured,
                "occurred_at": occurred,
            })

        first = receipt("r-1", "achieved", "1000", "2026-09-26T14:05:00Z")
        self.assertTrue(first["applied"])
        self.assertEqual(first["action_state"], "achieved")
        stale = receipt("r-2", "started", "0", "2026-09-26T14:01:00Z")
        self.assertFalse(stale["applied"])
        self.assertEqual(stale["note"], "stale_event_time")
        self.assertEqual(stale["action_state"], "achieved")
        refined = receipt("r-3", "partial", "900", "2026-09-26T14:06:00Z")
        self.assertTrue(refined["applied"])
        self.assertEqual(self._actions()[0]["actual_kw"], "900")
        regressive = receipt("r-4", "started", "0", "2026-09-26T14:07:00Z")
        self.assertFalse(regressive["applied"])
        self.assertEqual(regressive["note"], "regressive_transition")
        self.clock.advance(minutes=90)  # 15:40
        recovering = receipt("r-5", "recovery_started", "0", "2026-09-26T15:10:00Z")
        self.assertTrue(recovering["applied"])
        older = receipt("r-6", "achieved", "950", "2026-09-26T15:05:00Z")
        self.assertFalse(older["applied"])
        self.assertEqual(older["note"], "stale_event_time")
        recovered = receipt("r-7", "recovered", "0", "2026-09-26T15:30:00Z")
        self.assertTrue(recovered["applied"])
        self.assertEqual(self.service.resource_detail("storage-1")["state"], "available")
        protected = receipt("r-8", "failed", "0", "2026-09-26T15:35:00Z")
        self.assertFalse(protected["applied"])
        self.assertEqual(protected["note"], "terminal_protected")
        self.assertEqual(self._actions()[0]["state"], "recovered")

    def test_duplicate_receipt_replays_without_side_effects(self) -> None:
        self._event(curve=[{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}])
        self._confirm()
        action_id = self._actions()[0]["action_id"]
        self.clock.advance(minutes=70)
        payload = {"receipt_id": "r-1", "action_id": action_id, "status": "achieved", "measured_kw": "1000", "occurred_at": "2026-09-26T14:05:00Z"}
        first = self.service.record_receipt("disp", "DR-1", payload)
        second = self.service.record_receipt("disp", "DR-1", payload)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertTrue(second["applied"])
        count = self.connection.execute("SELECT COUNT(*) AS count FROM dr_receipts").fetchone()["count"]
        self.assertEqual(count, 1)

    def test_revision_keeps_executed_phases_and_replans_pending(self) -> None:
        self._event()
        self._confirm()
        self.clock.advance(minutes=70)  # 14:10
        for action in self._actions():
            if action["phase_seq"] == 0:
                self.service.record_receipt("disp", "DR-1", {
                    "receipt_id": f"r-{action['resource_id']}",
                    "action_id": action["action_id"],
                    "status": "achieved",
                    "measured_kw": action["planned_kw"],
                    "occurred_at": "2026-09-26T14:05:00Z",
                })
        self.clock.advance(minutes=40)  # 14:50
        with self.assertRaises(ValidationFailed):
            self.service.revise_event("plan", "DR-1", {"curve": [{"starts_at": "2026-09-26T14:30:00Z", "ends_at": "2026-09-26T15:30:00Z", "target_reduction_kw": "1000"}]})
        revision = self.service.revise_event("plan", "DR-1", {"curve": [{"starts_at": "2026-09-26T15:00:00Z", "ends_at": "2026-09-26T16:00:00Z", "target_reduction_kw": "2000"}]})
        self.assertEqual(revision["version"], 2)
        self.assertEqual(revision["superseded_phases"], [1])
        detail = self.service.event_detail("DR-1")
        phases = {phase["phase_seq"]: phase["state"] for phase in detail["phases"]}
        self.assertEqual(phases, {0: "executing", 1: "superseded", 2: "pending"})
        action_states = {(action["phase_seq"], action["resource_id"]): action["state"] for action in detail["actions"]}
        self.assertEqual(action_states[(0, "storage-1")], "achieved")
        self.assertEqual(action_states[(1, "storage-1")], "cancelled")
        # 第一阶段动作仍在执行，资源保持冻结，不能被新候选重复使用。
        self.assertEqual(self.service.resource_detail("storage-1")["state"], "frozen")
        run = self.service.generate_candidates("plan", "DR-1")
        self.assertEqual(run["version"], 2)
        for candidate in run["candidates"]:
            self.assertFalse(candidate["feasible"])
            self.assertEqual(candidate["actions"], [])
        self.service.register_resource("plan", "site-1", {"resource_id": "storage-2", "kind": "storage", "adjustable_kw": "1500", "ramp_kw_per_minute": "100", "energy_kwh": "4500"})
        self.service.register_resource("plan", "site-1", {"resource_id": "cap-b", "kind": "power_cap", "tenant_id": "tenant-b", "adjustable_kw": "1000", "ramp_kw_per_minute": "50"})
        rerun = self.service.generate_candidates("plan", "DR-1")
        best = rerun["candidates"][0]
        self.assertTrue(best["feasible"])
        confirmed = self.service.confirm_candidate("ops", "DR-1", best["candidate_id"], 2)
        self.assertEqual(sorted(confirmed["frozen_resources"]), ["cap-b", "storage-2"])

    def test_revision_requires_pending_phases(self) -> None:
        self._event(curve=[{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}])
        self.clock.advance(hours=3)  # 16:00，唯一阶段已经结束
        with self.assertRaises(InvalidState):
            self.service.revise_event("plan", "DR-1", {"curve": [{"starts_at": "2026-09-26T16:30:00Z", "ends_at": "2026-09-26T16:50:00Z", "target_reduction_kw": "100"}]})

    def test_report_explains_shortfall_tenants_and_recovery(self) -> None:
        self._event()
        self._confirm()
        self.clock.advance(minutes=70)  # 14:10
        actions = self._actions()
        by_resource = {action["resource_id"]: action for action in actions if action["phase_seq"] == 0}
        self.service.record_receipt("disp", "DR-1", {"receipt_id": "r-1", "action_id": by_resource["storage-1"]["action_id"], "status": "achieved", "measured_kw": "1666.666", "occurred_at": "2026-09-26T14:05:00Z"})
        self.service.record_receipt("disp", "DR-1", {"receipt_id": "r-2", "action_id": by_resource["cap-a"]["action_id"], "status": "achieved", "measured_kw": "600", "occurred_at": "2026-09-26T14:06:00Z"})
        self.service.record_receipt("disp", "DR-1", {"receipt_id": "r-3", "action_id": by_resource["delay-b"]["action_id"], "status": "partial", "measured_kw": "200", "occurred_at": "2026-09-26T14:06:00Z"})
        report = self.service.event_report("audit", "DR-1")
        self.assertEqual(report["baseline_kw"], "12000")
        self.assertEqual(report["totals"]["target_reduction_kw"], "5300.000")
        self.assertEqual(report["totals"]["planned_kw"], "5300.000")
        phase0 = next(phase for phase in report["phases"] if phase["phase_seq"] == 0)
        self.assertEqual(phase0["actual_kw"], "2466.666")
        self.assertEqual(phase0["actual_shortfall_kw"], "33.334")
        self.assertEqual(phase0["shortfall_reasons"], ["partial_delivery"])
        phase1 = next(phase for phase in report["phases"] if phase["phase_seq"] == 1)
        self.assertIsNone(phase1["actual_shortfall_kw"])
        self.assertEqual(phase1["shortfall_reasons"], ["awaiting_execution"])
        tenants = {item["tenant_id"]: item for item in report["tenant_impacts"]}
        self.assertEqual(tenants["tenant-a"]["actual_reduction_kw"], "600.000")
        self.assertEqual(tenants["tenant-a"]["delivered_load_kw"], "4400.000")
        self.assertFalse(tenants["tenant-a"]["floor_breached"])
        self.assertEqual(tenants["tenant-b"]["delayed_minutes"], "120.000")
        storage_recovery = next(item for item in report["recovery_plan"] if item["resource_id"] == "storage-1" and item["phase_seq"] == 0)
        self.assertEqual(storage_recovery["ramp_down_minutes"], 17)
        self.assertEqual(storage_recovery["recovery_eta"], "2026-09-26T15:17:00Z")
        self.assertTrue(storage_recovery["within_recovery_window"])

    def test_close_requires_all_phases_executed(self) -> None:
        self._event(curve=[{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}])
        self._confirm()
        with self.assertRaises(InvalidState):
            self.service.close_event("ops", "DR-1")
        action_id = self._actions()[0]["action_id"]
        self.clock.advance(minutes=70)
        self.service.record_receipt("disp", "DR-1", {"receipt_id": "r-1", "action_id": action_id, "status": "achieved", "measured_kw": "1000", "occurred_at": "2026-09-26T14:05:00Z"})
        self.clock.advance(minutes=60)
        self.service.record_receipt("disp", "DR-1", {"receipt_id": "r-2", "action_id": action_id, "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T15:10:00Z"})
        closed = self.service.close_event("ops", "DR-1")
        self.assertEqual(closed["state"], "closed")
        with self.assertRaises(InvalidState):
            self.service.revise_event("plan", "DR-1", {"curve": [{"starts_at": "2026-09-26T16:30:00Z", "ends_at": "2026-09-26T16:50:00Z", "target_reduction_kw": "100"}]})

    def test_permissions_are_enforced(self) -> None:
        self._event()
        with self.assertRaises(Forbidden):
            self.service.generate_candidates("ops", "DR-1")
        self.service.generate_candidates("plan", "DR-1")
        run = self.service.generate_candidates("plan", "DR-1")
        with self.assertRaises(Forbidden):
            self.service.confirm_candidate("plan", "DR-1", run["candidates"][0]["candidate_id"], 1)
        with self.assertRaises(Forbidden):
            self.service.record_receipt("plan", "DR-1", {"receipt_id": "r-x", "action_id": "a", "status": "achieved", "measured_kw": "1", "occurred_at": "2026-09-26T14:05:00Z"})
        with self.assertRaises(Forbidden):
            self.service.event_report("plan", "DR-1")
        with self.assertRaises(NotFound):
            self.service.event_report("nobody", "DR-1")

    def test_audit_chain_detects_tampering(self) -> None:
        self._event()
        self.assertTrue(self.service.audit_chain("audit")["valid"])
        self.connection.execute("UPDATE grid_audit_events SET payload_json='{}' WHERE event_id=1")
        self.assertFalse(self.service.audit_chain("audit")["valid"])


class GridApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        clock = FrozenClock(datetime(2026, 9, 26, 13, 0, tzinfo=timezone.utc))
        self.app = JsonApplication(GridResponseService(self.connection, clock))
        self.connection.execute(
            "INSERT INTO grid_users(user_id,display_name,role,created_at) VALUES('plan','规划','planner','2026-09-26T13:00:00Z')"
        )

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_event_flow_over_http(self) -> None:
        headers = {"X-Actor-Id": "plan"}
        site = self.app.handle("POST", "/sites", headers, json.dumps({
            "site_id": "site-1", "name": "园区", "timezone": "Asia/Shanghai", "baseline_kw": "8000",
        }).encode())
        self.assertEqual(site.status, 201)
        floor = self.app.handle("POST", "/sites/site-1/tenant-floors", headers, json.dumps({
            "tenant_id": "tenant-a", "current_load_kw": "3000", "min_capacity_kw": "1000",
        }).encode())
        self.assertEqual(floor.status, 201)
        resource = self.app.handle("POST", "/sites/site-1/resources", headers, json.dumps({
            "resource_id": "cap-a", "kind": "power_cap", "tenant_id": "tenant-a",
            "adjustable_kw": "1500", "ramp_kw_per_minute": "100",
        }).encode())
        self.assertEqual(resource.status, 201)
        event = self.app.handle("POST", "/events", headers, json.dumps({
            "event_id": "DR-1", "site_id": "site-1",
            "window_start": "2026-09-26T14:00:00Z", "window_end": "2026-09-26T15:00:00Z",
            "recovery_minutes": 30,
            "curve": [{"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "1000"}],
        }).encode())
        self.assertEqual(event.status, 201)
        run = self.app.handle("POST", "/events/DR-1/candidates", headers, b"{}")
        self.assertEqual(run.status, 200)
        self.assertEqual(len(run.body["candidates"]), 3)
        missing_actor = self.app.handle("GET", "/events/DR-1")
        self.assertEqual(missing_actor.status, 422)
        detail = self.app.handle("GET", "/events/DR-1", headers)
        self.assertEqual(detail.status, 200)
        self.assertEqual(detail.body["state"], "planned")
        unknown = self.app.handle("GET", "/events/DR-404", headers)
        self.assertEqual(unknown.status, 404)


class AcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = acceptance_run(ROOT)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["state"], "closed")
        self.assertEqual(result["version"], 2)
        self.assertTrue(result["first_run_replayed"])
        self.assertTrue(result["receipt_replayed"])
        self.assertEqual(result["receipt_notes"]["stale_start"], "stale_event_time")
        self.assertEqual(result["receipt_notes"]["terminal_protected"], "terminal_protected")
        self.assertEqual(result["phases"], {0: "executed", 2: "executed"})
        self.assertEqual(result["totals"]["target_reduction_kw"], "4500.000")
        self.assertEqual(result["totals"]["actual_kw"], "4466.666")
        self.assertEqual(result["shortfall_reasons"], ["partial_delivery"])
        self.assertTrue(result["recovery_within_window"])
        self.assertTrue(result["resources_released"])
        self.assertTrue(result["audit"]["valid"])


if __name__ == "__main__":
    unittest.main()
