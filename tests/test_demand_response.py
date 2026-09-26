from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from demand_response.api import JsonApplication
from demand_response.clock import FrozenClock
from demand_response.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from demand_response.planning import build_plan, q
from demand_response.service import DemandResponseService


def stage(stage_id: str, target: str, start_hour: int = 13) -> dict[str, object]:
    return {
        "stage_id": stage_id,
        "starts_at": f"2026-09-25T{start_hour:02d}:00:00Z",
        "ends_at": f"2026-09-25T{start_hour + 1:02d}:00:00Z",
        "target_reduction_kw": target,
    }


class PlanningTests(unittest.TestCase):
    def plan(self, stages, resources, baselines=None, guarantees=None, recover_by=None, **kwargs):
        stages = stages if isinstance(stages, list) else [stages]
        stage_ids = [item["stage_id"] for item in stages]
        if baselines is None:
            baselines = {("site-a", sid): Decimal("1000") for sid in stage_ids}
        return build_plan(
            directive_id="dr-1",
            source_revision="rev-1",
            window_end=stages[-1]["ends_at"],
            recover_by_at=recover_by or "2026-09-25T17:00:00Z",
            stages_raw=stages,
            baselines=baselines,
            guarantees=guarantees or {},
            raw_resources=resources,
            **kwargs,
        )

    def test_protected_job_never_delayed(self) -> None:
        resources = [
            {"resource_id": "job-protected", "site_id": "site-a", "kind": "job", "tenant_id": "t1",
             "detail": {"tenant_id": "t1", "load_kw": "100", "stages": ["s1"], "interruptible": False,
                        "resume_by_at": "2026-09-25T17:00:00Z", "duration_minutes": "60"}},
        ]
        plan = self.plan(stage("s1", "100"), resources)
        self.assertFalse(plan["feasible"])
        self.assertEqual(plan["stages"][0]["actions"], [])
        self.assertEqual(plan["stages"][0]["reasons"][0]["code"], "protected_job")

    def test_tenant_guarantee_floors_power_cap(self) -> None:
        resources = [
            {"resource_id": "cap-1", "site_id": "site-a", "kind": "cap", "tenant_id": "t1",
             "detail": {"tenant_id": "t1", "current_kw": "300", "max_reduction_kw": "200",
                        "ramp_kw_per_stage": "200", "floor_kw": "100"}},
        ]
        plan = self.plan(
            stage("s1", "200"), resources,
            guarantees={("site-a", "t1"): Decimal("200")},
        )
        # 当前 300、保底 200，最多只能封顶 100。
        self.assertFalse(plan["feasible"])
        self.assertEqual(plan["stages"][0]["planned_reduction_kw"], "100.000")
        codes = {item["code"] for item in plan["stages"][0]["reasons"]}
        self.assertIn("tenant_guarantee", codes)

    def test_ramp_limits_increment_between_stages(self) -> None:
        resources = [
            {"resource_id": "cap-1", "site_id": "site-a", "kind": "cap", "tenant_id": "t1",
             "detail": {"tenant_id": "t1", "current_kw": "500", "max_reduction_kw": "300",
                        "ramp_kw_per_stage": "100", "floor_kw": "0"}},
        ]
        plan = self.plan([stage("s1", "100", 13), stage("s2", "300", 14)], resources)
        self.assertEqual(plan["stages"][0]["planned_reduction_kw"], "100.000")
        self.assertEqual(plan["stages"][1]["planned_reduction_kw"], "200.000")
        self.assertFalse(plan["stages"][1]["met"])
        self.assertEqual(plan["stages"][1]["reasons"][0]["code"], "ramp")

    def test_storage_recovery_window_binds_energy(self) -> None:
        # 电池 500 kWh、放电 500 kW，但充电只有 100 kW；恢复窗口 1 小时只能充回 100 kWh。
        resources = [
            {"resource_id": "ess-1", "site_id": "site-a", "kind": "storage", "tenant_id": None,
             "detail": {"available_kwh": "500", "discharge_kw": "500", "ramp_kw_per_stage": "500",
                        "recharge_kw": "100"}},
        ]
        plan = self.plan(stage("s1", "500"), resources,
                         recover_by="2026-09-25T15:00:00Z")
        self.assertEqual(plan["stages"][0]["planned_reduction_kw"], "100.000")
        self.assertEqual(plan["stages"][0]["reasons"][0]["code"], "recovery_window")
        self.assertTrue(plan["recovery_plan"]["storage"][0]["within_recovery_window"])

    def test_initial_state_carries_ramp_and_energy_after_revision(self) -> None:
        resources = [
            {"resource_id": "ess-1", "site_id": "site-a", "kind": "storage", "tenant_id": None,
             "detail": {"available_kwh": "500", "discharge_kw": "300", "ramp_kw_per_stage": "100",
                        "recharge_kw": "300"}},
        ]
        plan = self.plan(
            stage("s2", "300", 14), resources,
            baselines={("site-a", "s2"): Decimal("1000")},
            storage_initial={"ess-1": {"reduction_kw": Decimal("100"), "discharged_kwh": Decimal("100")}},
        )
        # 已锁阶段放了 100 kW；本阶段最多再爬坡 100 kW，且只剩 400 kWh 电量。
        self.assertEqual(plan["stages"][0]["planned_reduction_kw"], "200.000")
        self.assertEqual(plan["stages"][0]["reasons"][0]["code"], "ramp")


def directive_payload(targets=("300", "300"), revision="grid-rev-1", key="reg-1") -> dict[str, object]:
    return {
        "directive_id": "dr-1",
        "source_revision": revision,
        "window_start": "2026-09-25T13:00:00Z",
        "window_end": "2026-09-25T15:00:00Z",
        "recover_by_at": "2026-09-25T17:00:00Z",
        "note": "晚峰响应",
        "idempotency_key": key,
        "curve": [
            stage("stage-1", targets[0]),
            stage("stage-2", targets[1], 14),
        ],
        "baselines": [
            {"site_id": "site-a", "stage_id": "stage-1", "baseline_kw": "1000"},
            {"site_id": "site-a", "stage_id": "stage-2", "baseline_kw": "1000"},
        ],
    }


def job_payload(resource_id: str, load: str, interruptible: bool, stages=("stage-1", "stage-2")) -> dict[str, object]:
    return {
        "directive_id": "dr-1", "resource_id": resource_id, "site_id": "site-a", "kind": "job",
        "detail": {"tenant_id": "tenant-1", "load_kw": load, "stages": list(stages),
                   "interruptible": interruptible, "resume_by_at": "2026-09-25T17:00:00Z",
                   "duration_minutes": "60"},
    }


def receipt(event_id: str, resource: str, event_type: str, event_time: str, observed=None,
            stage="stage-1") -> dict[str, object]:
    payload = {
        "directive_id": "dr-1", "source_event_id": event_id, "stage_id": stage,
        "resource_id": resource, "event_type": event_type, "event_time": event_time,
    }
    if observed is not None:
        payload["observed_reduction_kw"] = observed
    return payload


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc))
        self.service = DemandResponseService(self.connection, self.clock)
        for user_id, role in (("ops", "operator"), ("lead", "approver"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)

    def tearDown(self) -> None:
        self.connection.close()

    def _bootstrap(self, targets=("300", "300")) -> int:
        self.service.register_directive("ops", directive_payload(targets))
        self.service.register_resource("ops", job_payload("job-1", "120", True))
        self.service.register_resource("ops", job_payload("job-protected", "100", False))
        self.service.register_resource("ops", {
            "directive_id": "dr-1", "resource_id": "cap-1", "site_id": "site-a", "kind": "cap",
            "detail": {"tenant_id": "tenant-1", "current_kw": "400", "max_reduction_kw": "300",
                       "ramp_kw_per_stage": "300", "floor_kw": "0"},
        })
        self.service.register_resource("ops", {
            "directive_id": "dr-1", "resource_id": "ess-1", "site_id": "site-a", "kind": "storage",
            "detail": {"available_kwh": "600", "discharge_kw": "300", "ramp_kw_per_stage": "300",
                       "recharge_kw": "300"},
        })
        candidate = self.service.generate_candidates("ops", "dr-1")
        self.assertTrue(candidate["feasible"], candidate["plan"])
        frozen = self.service.confirm_candidate("lead", candidate["candidate_id"])
        return frozen["candidate_id"]

    def test_directive_validation_rejects_gap_in_curve(self) -> None:
        payload = directive_payload()
        payload["curve"][1]["starts_at"] = "2026-09-25T14:30:00Z"
        with self.assertRaises(ValidationFailed):
            self.service.register_directive("ops", payload)

    def test_infeasible_candidate_cannot_be_frozen(self) -> None:
        self.service.register_directive("ops", directive_payload(("900", "900")))
        self.service.register_resource("ops", job_payload("job-1", "120", True))
        candidate = self.service.generate_candidates("ops", "dr-1")
        self.assertFalse(candidate["feasible"])
        with self.assertRaises(InvalidState):
            self.service.confirm_candidate("lead", candidate["candidate_id"])

    def test_only_approver_confirms_and_freezes_once(self) -> None:
        candidate_id = self._bootstrap()
        with self.assertRaises(Forbidden):
            self.service.confirm_candidate("ops", candidate_id)
        with self.assertRaises(InvalidState):
            self.service.confirm_candidate("lead", candidate_id)

    def test_revision_only_affects_unlocked_stages(self) -> None:
        self._bootstrap()
        self.service.ingest_receipt("ops", receipt("e1", "job-1", "dispatched", "2026-09-25T13:02:00Z"))
        self.service.ingest_receipt("ops", receipt("e2", "job-1", "achieved", "2026-09-25T13:05:00Z", "120"))
        # 已锁阶段曲线不允许改动。
        changed = directive_payload(("310", "280"), revision="grid-rev-2", key="rev-1")
        with self.assertRaises(Conflict):
            self.service.revise_directive("ops", changed)
        payload = directive_payload(("300", "280"), revision="grid-rev-2", key="rev-1")
        revised = self.service.revise_directive("ops", payload)
        self.assertEqual((revised["version"], revised["locked_stages"]), (2, ["stage-1"]))
        candidate = self.service.generate_candidates("ops", "dr-1")
        self.assertEqual(candidate["version"], 2)
        self.assertTrue(candidate["plan"]["feasible"])
        self.service.confirm_candidate("lead", candidate["candidate_id"])
        active = self.connection.execute(
            "SELECT stage_id,state FROM dr_frozen_actions WHERE resource_id='job-1' ORDER BY freeze_id"
        ).fetchall()
        # stage-1 的冻结动作保持生效，旧版本 stage-2 动作作废后由新版本重建。
        states = {(row["stage_id"], row["state"]) for row in active}
        self.assertIn(("stage-1", "achieved"), states)
        self.assertTrue(any(stage == "stage-2" and state == "locked" for stage, state in states))
        self.assertTrue(any(stage == "stage-2" and state == "superseded" for stage, state in states))

    def test_duplicate_and_out_of_order_receipts_are_merged(self) -> None:
        self._bootstrap()
        self.service.ingest_receipt("ops", receipt("d1", "job-1", "dispatched", "2026-09-25T13:02:00Z"))
        early = self.service.ingest_receipt(
            "ops", receipt("a1-early", "job-1", "achieved", "2026-09-25T13:01:00Z", "120"))
        self.assertEqual(early["ignored_reason"], "stale_event_time")
        self.service.ingest_receipt("ops", receipt("a1", "job-1", "achieved", "2026-09-25T13:05:00Z", "120"))
        # 同一 source_event_id 原样重放：幂等返回已应用结果。
        duplicate = self.service.ingest_receipt(
            "ops", receipt("a1", "job-1", "achieved", "2026-09-25T13:05:00Z", "120"))
        self.assertTrue(duplicate["duplicate"])
        self.assertTrue(duplicate["applied"])
        # achieved 终态不允许 failed 覆盖。
        override = self.service.ingest_receipt(
            "ops", receipt("f1", "job-1", "failed", "2026-09-25T13:06:00Z"))
        self.assertEqual(override["ignored_reason"], "terminal_state_protected")
        # 未知资源回执被忽略但留痕。
        unknown = self.service.ingest_receipt(
            "ops", receipt("x1", "missing", "achieved", "2026-09-25T13:05:00Z", "10"))
        self.assertEqual(unknown["ignored_reason"], "no_frozen_action")
        row = self.connection.execute(
            "SELECT state,observed_reduction_kw FROM dr_frozen_actions "
            "WHERE stage_id='stage-1' AND resource_id='job-1' AND state<>'superseded'"
        ).fetchone()
        self.assertEqual((row["state"], row["observed_reduction_kw"]), ("achieved", "120"))

    def test_report_explains_shortfall_and_recovery(self) -> None:
        # 目标 500 kW：作业 120 + 封顶 300 不足，必须动用储能。
        self.service.register_directive("ops", directive_payload(("500", "500")))
        self.service.register_resource("ops", job_payload("job-1", "120", True))
        self.service.register_resource("ops", job_payload("job-protected", "100", False))
        self.service.register_resource("ops", {
            "directive_id": "dr-1", "resource_id": "cap-1", "site_id": "site-a", "kind": "cap",
            "detail": {"tenant_id": "tenant-1", "current_kw": "400", "max_reduction_kw": "300",
                       "ramp_kw_per_stage": "300", "floor_kw": "0"},
        })
        self.service.register_resource("ops", {
            "directive_id": "dr-1", "resource_id": "ess-1", "site_id": "site-a", "kind": "storage",
            "detail": {"available_kwh": "600", "discharge_kw": "300", "ramp_kw_per_stage": "300",
                       "recharge_kw": "300"},
        })
        candidate = self.service.generate_candidates("ops", "dr-1")
        self.assertTrue(candidate["feasible"], candidate["plan"])
        self.service.confirm_candidate("lead", candidate["candidate_id"])
        for resource in ("job-1", "cap-1", "ess-1"):
            self.service.ingest_receipt(
                "ops", receipt(f"d-{resource}", resource, "dispatched", "2026-09-25T13:02:00Z"))
            self.service.ingest_receipt(
                "ops", receipt(f"a-{resource}", resource, "achieved", "2026-09-25T13:05:00Z", "50"))
        for resource in ("job-1", "cap-1", "ess-1"):
            self.service.ingest_receipt(
                "ops", receipt(f"d2-{resource}", resource, "dispatched", "2026-09-25T14:02:00Z", stage="stage-2"))
            self.service.ingest_receipt(
                "ops", receipt(f"a2-{resource}", resource, "achieved", "2026-09-25T14:05:00Z", "50", stage="stage-2"))
        report = self.service.directive_report("audit", "dr-1")
        stage1 = report["stages"][0]
        self.assertFalse(stage1["met"])
        self.assertTrue(any(item["code"] == "under_delivery" for item in stage1["shortfall_reasons"]))
        self.assertEqual(report["tenant_impact"][0]["tenant_id"], "tenant-1")
        self.assertIsNotNone(report["totals"]["actual_kwh"])
        self.assertTrue(report["recovery_plan"]["storage"])


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(DemandResponseService(self.connection))
        for user_id, role in (("ops", "operator"), ("lead", "approver"), ("audit", "auditor")):
            self.app.handle("POST", "/users", body=json.dumps(
                {"user_id": user_id, "display_name": user_id, "role": role}).encode())

    def tearDown(self) -> None:
        self.connection.close()

    def test_health_and_actor_header(self) -> None:
        self.assertEqual(self.app.handle("GET", "/health").body["status"], "ok")
        response = self.app.handle("POST", "/directives", body=b"{}")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_register_and_report_route(self) -> None:
        payload = json.dumps(directive_payload()).encode()
        response = self.app.handle("POST", "/directives", headers={"X-Actor-Id": "ops"}, body=payload)
        self.assertEqual(response.status, 201)
        report = self.app.handle(
            "GET", "/directives/dr-1/report", headers={"X-Actor-Id": "audit"})
        self.assertEqual(report.status, 200)
        self.assertEqual(report.body["current_version"], 1)
        missing = self.app.handle(
            "GET", "/directives/nope/report", headers={"X-Actor-Id": "audit"})
        self.assertEqual(missing.status, 404)


if __name__ == "__main__":
    unittest.main()
