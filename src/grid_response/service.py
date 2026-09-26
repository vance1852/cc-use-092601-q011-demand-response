"""需求响应指令、候选组合、资源冻结与执行回执的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    EventInput,
    ReceiptInput,
    ResourceInput,
    SiteInput,
    TenantFloorInput,
    parse_curve,
)
from .planning import (
    PhaseInput,
    ResourceInput as PlanResource,
    TenantFloorInput as PlanFloor,
    achievement_percent,
    canonical_json,
    decimal_text,
    digest,
    generate_candidates as plan_candidates,
    quantize_kw,
    recovery_minutes_needed,
    shortfall_reasons,
)
from .storage import initialize, transaction


ZERO = Decimal("0")

ROLE_PERMISSIONS = {
    "planner": {"site.write", "resource.write", "event.write", "candidate.generate"},
    "operator": {"event.confirm", "event.close"},
    "dispatcher": {"receipt.write"},
    "auditor": {"report.read", "audit.read"},
}

# 回执状态 -> (动作状态, 进度秩)；秩用于按事件时间做前向归并。
RECEIPT_TARGET = {
    "started": ("executing", 1),
    "achieved": ("achieved", 2),
    "partial": ("partial", 2),
    "failed": ("failed", 2),
    "recovery_started": ("recovering", 3),
    "recovered": ("recovered", 4),
}
ACTION_RANK = {
    "frozen": 0,
    "executing": 1,
    "achieved": 2,
    "partial": 2,
    "failed": 2,
    "recovering": 3,
    "recovered": 4,
    "cancelled": 99,
}
TERMINAL_STATES = {"recovered", "cancelled"}
MEASURED_STATUSES = {"achieved", "partial", "failed"}
RELEASE_STATES = {"recovered", "failed"}


class GridResponseService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM grid_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM grid_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO grid_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO grid_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    # ------------------------------------------------------------------
    # 站点基线与资源可调属性
    # ------------------------------------------------------------------

    def _site(self, site_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dr_sites WHERE site_id=?", (site_id,)
        ).fetchone()
        if row is None:
            raise NotFound("站点不存在")
        return row

    def register_site(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "site.write")
        site = SiteInput.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dr_sites(site_id,name,timezone,baseline_kw,created_at) VALUES(?,?,?,?,?)",
                    (site.site_id, site.name, site.timezone, decimal_text(site.baseline_kw), self._now()),
                )
                self._audit("site", site.site_id, "site.registered", actor_id, dict(raw))
        except sqlite3.IntegrityError as exc:
            raise Conflict("站点编号已经存在") from exc
        return self.site_detail(site.site_id)

    def site_detail(self, site_id: str) -> dict[str, Any]:
        row = self._site(site_id)
        result = dict(row)
        floors = self.connection.execute(
            "SELECT * FROM dr_tenant_floors WHERE site_id=? ORDER BY tenant_id", (site_id,)
        ).fetchall()
        result["tenant_floors"] = [dict(floor) for floor in floors]
        return result

    def update_baseline(self, actor_id: str, site_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "site.write")
        self._site(site_id)
        baseline = decimal_text(SiteInput.from_dict({
            "site_id": site_id,
            "name": "baseline",
            "timezone": "UTC",
            "baseline_kw": raw.get("baseline_kw"),
        }).baseline_kw)
        expected = raw.get("expected_revision")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected <= 0:
            raise ValidationFailed("expected_revision 必须是正整数")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE dr_sites SET baseline_kw=?,revision=revision+1 WHERE site_id=? AND revision=?",
                (baseline, site_id, expected),
            )
            if cursor.rowcount != 1:
                raise Conflict("站点基线版本不匹配")
            self._audit("site", site_id, "site.baseline_updated", actor_id, {"baseline_kw": baseline})
        return self.site_detail(site_id)

    def register_tenant_floor(self, actor_id: str, site_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "resource.write")
        self._site(site_id)
        floor = TenantFloorInput.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dr_tenant_floors(site_id,tenant_id,current_load_kw,min_capacity_kw,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (
                        site_id,
                        floor.tenant_id,
                        decimal_text(floor.current_load_kw),
                        decimal_text(floor.min_capacity_kw),
                        self._now(),
                    ),
                )
                self._audit("site", site_id, "tenant_floor.registered", actor_id, dict(raw))
        except sqlite3.IntegrityError as exc:
            raise Conflict("租户最低算力已经登记") from exc
        return {"site_id": site_id, "tenant_id": floor.tenant_id}

    def register_resource(self, actor_id: str, site_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "resource.write")
        self._site(site_id)
        resource = ResourceInput.from_dict(raw)
        if resource.tenant_id is not None:
            floor = self.connection.execute(
                "SELECT 1 FROM dr_tenant_floors WHERE site_id=? AND tenant_id=?",
                (site_id, resource.tenant_id),
            ).fetchone()
            if floor is None:
                raise ValidationFailed("租户尚未登记最低算力基线")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dr_resources(resource_id,site_id,kind,tenant_id,adjustable_kw,"
                    "ramp_kw_per_minute,max_delay_minutes,energy_kwh,protected,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        resource.resource_id,
                        site_id,
                        resource.kind,
                        resource.tenant_id,
                        decimal_text(resource.adjustable_kw),
                        decimal_text(resource.ramp_kw_per_minute),
                        resource.max_delay_minutes,
                        None if resource.energy_kwh is None else decimal_text(resource.energy_kwh),
                        1 if resource.protected else 0,
                        self._now(),
                    ),
                )
                self._audit("resource", resource.resource_id, "resource.registered", actor_id, dict(raw))
        except sqlite3.IntegrityError as exc:
            raise Conflict("资源编号已经存在") from exc
        return self.resource_detail(resource.resource_id)

    def resource_detail(self, resource_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM dr_resources WHERE resource_id=?", (resource_id,)
        ).fetchone()
        if row is None:
            raise NotFound("可调资源不存在")
        return dict(row)

    # ------------------------------------------------------------------
    # 指令登记与修订
    # ------------------------------------------------------------------

    def _release_resource_if_idle(self, event_id: str, resource_id: str) -> None:
        """资源在同一指令下可能服务多个阶段；全部动作完结后才解冻。"""
        active = self.connection.execute(
            "SELECT COUNT(*) AS count FROM dr_actions WHERE event_id=? AND resource_id=? "
            "AND state NOT IN ('recovered','failed','cancelled')",
            (event_id, resource_id),
        ).fetchone()["count"]
        if active:
            return
        self.connection.execute(
            "UPDATE dr_resources SET state='available',frozen_event_id=NULL,revision=revision+1 "
            "WHERE resource_id=? AND state='frozen' AND frozen_event_id=?",
            (resource_id, event_id),
        )

    def _current_event(self, event_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dr_events WHERE event_id=? ORDER BY version DESC LIMIT 1", (event_id,)
        ).fetchone()
        if row is None:
            raise NotFound("需求响应指令不存在")
        return row

    def _phases(self, event_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM dr_phases WHERE event_id=? ORDER BY phase_seq", (event_id,)
        ).fetchall()

    @staticmethod
    def _curve_rows(curve) -> list[dict[str, str]]:
        return [
            {
                "starts_at": interval.starts_at,
                "ends_at": interval.ends_at,
                "target_reduction_kw": decimal_text(interval.target_reduction_kw),
            }
            for interval in curve
        ]

    def register_event(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "event.write")
        event = EventInput.from_dict(raw)
        site = self._site(event.site_id)
        now = self.clock.now()
        window_start = parse_utc(event.window_start, "window_start")
        window_end = parse_utc(event.window_end, "window_end")
        if window_end <= window_start:
            raise ValidationFailed("window_end 必须晚于 window_start")
        if window_start < now:
            raise ValidationFailed("承诺窗口必须不早于当前时间")
        first, last = event.curve[0], event.curve[-1]
        if parse_utc(first.starts_at) < window_start or parse_utc(last.ends_at) > window_end:
            raise ValidationFailed("目标削减曲线必须位于承诺窗口内")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dr_events(event_id,version,site_id,window_start,window_end,recovery_minutes,"
                    "baseline_kw,curve_json,state,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event.event_id,
                        1,
                        event.site_id,
                        event.window_start,
                        event.window_end,
                        event.recovery_minutes,
                        site["baseline_kw"],
                        canonical_json(self._curve_rows(event.curve)),
                        "registered",
                        actor_id,
                        self._now(),
                    ),
                )
                for seq, interval in enumerate(event.curve):
                    self.connection.execute(
                        "INSERT INTO dr_phases(event_id,phase_seq,version_introduced,starts_at,ends_at,"
                        "target_reduction_kw) VALUES(?,?,?,?,?,?)",
                        (
                            event.event_id,
                            seq,
                            1,
                            interval.starts_at,
                            interval.ends_at,
                            decimal_text(interval.target_reduction_kw),
                        ),
                    )
                self._audit(
                    "event",
                    event.event_id,
                    "event.registered",
                    actor_id,
                    {"version": 1, "phases": len(event.curve)},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("指令编号已经存在") from exc
        return {"event_id": event.event_id, "version": 1, "state": "registered", "phases": len(event.curve)}

    def revise_event(self, actor_id: str, event_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "event.write")
        current = self._current_event(event_id)
        if current["state"] == "closed":
            raise InvalidState("指令已关闭，不能修订")
        curve = parse_curve(raw.get("curve"))
        now = self.clock.now()
        phases = self._phases(event_id)
        locked = [
            phase
            for phase in phases
            if phase["state"] in ("executing", "executed") or parse_utc(phase["ends_at"]) <= now
        ]
        pending = [
            phase
            for phase in phases
            if phase["state"] == "pending" and parse_utc(phase["ends_at"]) > now
        ]
        if not pending:
            raise InvalidState("没有未执行阶段可修订")
        window_start = parse_utc(current["window_start"])
        window_end = parse_utc(current["window_end"])
        for interval in curve:
            start = parse_utc(interval.starts_at)
            end = parse_utc(interval.ends_at)
            if start <= now:
                raise ValidationFailed("修订曲线必须全部位于当前时间之后")
            if start < window_start or end > window_end:
                raise ValidationFailed("修订曲线必须位于承诺窗口内")
            for phase in locked:
                if start < parse_utc(phase["ends_at"]) and parse_utc(phase["starts_at"]) < end:
                    raise ValidationFailed("修订曲线与已执行阶段重叠")
        new_version = int(current["version"]) + 1
        next_seq = max(phase["phase_seq"] for phase in phases) + 1
        pending_seqs = [phase["phase_seq"] for phase in pending]
        with transaction(self.connection, immediate=True):
            placeholders = ",".join("?" for _ in pending_seqs)
            released = self.connection.execute(
                f"SELECT DISTINCT resource_id FROM dr_actions WHERE event_id=? AND state='frozen' "
                f"AND phase_seq IN ({placeholders})",
                (event_id, *pending_seqs),
            ).fetchall()
            self.connection.execute(
                f"UPDATE dr_phases SET state='superseded' WHERE event_id=? AND phase_seq IN ({placeholders})",
                (event_id, *pending_seqs),
            )
            self.connection.execute(
                f"UPDATE dr_actions SET state='cancelled' WHERE event_id=? AND state='frozen' "
                f"AND phase_seq IN ({placeholders})",
                (event_id, *pending_seqs),
            )
            for row in released:
                self._release_resource_if_idle(event_id, row["resource_id"])
            self.connection.execute(
                "UPDATE dr_candidates SET state='cancelled' WHERE event_id=? AND version=? AND state='proposed'",
                (event_id, current["version"]),
            )
            self.connection.execute(
                "UPDATE dr_candidates SET state='superseded' WHERE event_id=? AND version=? AND state='confirmed'",
                (event_id, current["version"]),
            )
            self.connection.execute(
                "INSERT INTO dr_events(event_id,version,site_id,window_start,window_end,recovery_minutes,"
                "baseline_kw,curve_json,state,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    new_version,
                    current["site_id"],
                    current["window_start"],
                    current["window_end"],
                    current["recovery_minutes"],
                    current["baseline_kw"],
                    canonical_json(self._curve_rows(curve)),
                    "registered",
                    actor_id,
                    self._now(),
                ),
            )
            for offset, interval in enumerate(curve):
                self.connection.execute(
                    "INSERT INTO dr_phases(event_id,phase_seq,version_introduced,starts_at,ends_at,"
                    "target_reduction_kw) VALUES(?,?,?,?,?,?)",
                    (
                        event_id,
                        next_seq + offset,
                        new_version,
                        interval.starts_at,
                        interval.ends_at,
                        decimal_text(interval.target_reduction_kw),
                    ),
                )
            self._audit(
                "event",
                event_id,
                "event.revised",
                actor_id,
                {
                    "version": new_version,
                    "superseded_phases": pending_seqs,
                    "new_phases": [next_seq + offset for offset in range(len(curve))],
                },
            )
        return {
            "event_id": event_id,
            "version": new_version,
            "state": "registered",
            "superseded_phases": pending_seqs,
            "new_phases": [next_seq + offset for offset in range(len(curve))],
        }

    # ------------------------------------------------------------------
    # 候选组合生成与确认冻结
    # ------------------------------------------------------------------

    def _committed_storage_energy(self, event_id: str) -> dict[str, Decimal]:
        rows = self.connection.execute(
            "SELECT resource_id,planned_kw,starts_at,ends_at FROM dr_actions "
            "WHERE event_id=? AND kind='storage' AND state != 'cancelled'",
            (event_id,),
        ).fetchall()
        committed: dict[str, Decimal] = {}
        for row in rows:
            hours = Decimal(
                str((parse_utc(row["ends_at"]) - parse_utc(row["starts_at"])).total_seconds())
            ) / Decimal(3600)
            committed[row["resource_id"]] = committed.get(row["resource_id"], ZERO) + (
                Decimal(row["planned_kw"]) * hours
            )
        return committed

    def generate_candidates(self, actor_id: str, event_id: str) -> dict[str, Any]:
        self._require(actor_id, "candidate.generate")
        current = self._current_event(event_id)
        if current["state"] not in ("registered", "planned"):
            raise InvalidState("当前指令状态不能生成候选组合")
        now = self.clock.now()
        window_end = parse_utc(current["window_end"])
        pending = [
            phase
            for phase in self._phases(event_id)
            if phase["state"] == "pending" and parse_utc(phase["ends_at"]) > now
        ]
        if not pending:
            raise InvalidState("没有待执行阶段")
        resources = self.connection.execute(
            "SELECT * FROM dr_resources WHERE site_id=? AND state='available' ORDER BY resource_id",
            (current["site_id"],),
        ).fetchall()
        floors = self.connection.execute(
            "SELECT * FROM dr_tenant_floors WHERE site_id=? ORDER BY tenant_id",
            (current["site_id"],),
        ).fetchall()
        committed = self._committed_storage_energy(event_id)
        input_payload = {
            "event_id": event_id,
            "version": current["version"],
            "recovery_minutes": current["recovery_minutes"],
            "phases": [
                {
                    "phase_seq": phase["phase_seq"],
                    "starts_at": phase["starts_at"],
                    "ends_at": phase["ends_at"],
                    "target_reduction_kw": phase["target_reduction_kw"],
                }
                for phase in pending
            ],
            "resources": [
                {
                    "resource_id": row["resource_id"],
                    "kind": row["kind"],
                    "tenant_id": row["tenant_id"],
                    "adjustable_kw": row["adjustable_kw"],
                    "ramp_kw_per_minute": row["ramp_kw_per_minute"],
                    "max_delay_minutes": row["max_delay_minutes"],
                    "energy_kwh": row["energy_kwh"],
                    "protected": row["protected"],
                }
                for row in resources
            ],
            "floors": [
                {
                    "tenant_id": row["tenant_id"],
                    "current_load_kw": row["current_load_kw"],
                    "min_capacity_kw": row["min_capacity_kw"],
                }
                for row in floors
            ],
            "committed_energy_kwh": {key: decimal_text(value) for key, value in sorted(committed.items())},
        }
        input_sha256 = digest(input_payload)
        existing = self.connection.execute(
            "SELECT run_id FROM dr_candidate_runs WHERE event_id=? AND version=? AND input_sha256=?",
            (event_id, current["version"], input_sha256),
        ).fetchone()
        if existing is not None:
            return {
                "run_id": existing["run_id"],
                "event_id": event_id,
                "version": current["version"],
                "candidates": self._run_candidates(existing["run_id"]),
                "replayed": True,
            }
        phase_inputs = [
            PhaseInput(
                phase_seq=phase["phase_seq"],
                starts_at=parse_utc(phase["starts_at"]),
                ends_at=parse_utc(phase["ends_at"]),
                target_reduction_kw=Decimal(phase["target_reduction_kw"]),
                is_final=parse_utc(phase["ends_at"]) == window_end,
            )
            for phase in pending
        ]
        resource_inputs = [
            PlanResource(
                resource_id=row["resource_id"],
                kind=row["kind"],
                tenant_id=row["tenant_id"],
                adjustable_kw=Decimal(row["adjustable_kw"]),
                ramp_kw_per_minute=Decimal(row["ramp_kw_per_minute"]),
                max_delay_minutes=row["max_delay_minutes"],
                energy_kwh=None if row["energy_kwh"] is None else Decimal(row["energy_kwh"]),
                protected=bool(row["protected"]),
            )
            for row in resources
        ]
        floor_inputs = [
            PlanFloor(
                tenant_id=row["tenant_id"],
                current_load_kw=Decimal(row["current_load_kw"]),
                min_capacity_kw=Decimal(row["min_capacity_kw"]),
            )
            for row in floors
        ]
        planned = plan_candidates(
            phases=phase_inputs,
            resources=resource_inputs,
            floors=floor_inputs,
            recovery_minutes=int(current["recovery_minutes"]),
            committed_energy_kwh=committed,
        )
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE dr_candidates SET state='cancelled' WHERE event_id=? AND version=? AND state='proposed'",
                (event_id, current["version"]),
            )
            cursor = self.connection.execute(
                "INSERT INTO dr_candidate_runs(event_id,version,input_sha256,created_by,created_at) "
                "VALUES(?,?,?,?,?)",
                (event_id, current["version"], input_sha256, actor_id, self._now()),
            )
            run_id = int(cursor.lastrowid)
            for candidate in planned:
                candidate_id = f"{event_id}:v{current['version']}:r{run_id}:{candidate['strategy']}"
                summary = {
                    "phases": candidate["phases"],
                    "total_planned_kw": candidate["total_planned_kw"],
                    "total_shortfall_kw": candidate["total_shortfall_kw"],
                    "tenant_impact_kw": candidate["tenant_impact_kw"],
                }
                self.connection.execute(
                    "INSERT INTO dr_candidates(candidate_id,run_id,event_id,version,strategy,actions_json,"
                    "summary_json,feasible,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        candidate_id,
                        run_id,
                        event_id,
                        current["version"],
                        candidate["strategy"],
                        canonical_json(candidate["actions"]),
                        canonical_json(summary),
                        1 if candidate["feasible"] else 0,
                        self._now(),
                    ),
                )
            self.connection.execute(
                "UPDATE dr_events SET state='planned' WHERE event_id=? AND version=?",
                (event_id, current["version"]),
            )
            self._audit(
                "event",
                event_id,
                "candidates.generated",
                actor_id,
                {"run_id": run_id, "version": current["version"]},
            )
        return {
            "run_id": run_id,
            "event_id": event_id,
            "version": current["version"],
            "candidates": self._run_candidates(run_id),
            "replayed": False,
        }

    def _run_candidates(self, run_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM dr_candidates WHERE run_id=? ORDER BY candidate_id", (run_id,)
        ).fetchall()
        result = []
        for row in rows:
            summary = json.loads(row["summary_json"])
            result.append({
                "candidate_id": row["candidate_id"],
                "strategy": row["strategy"],
                "state": row["state"],
                "feasible": bool(row["feasible"]),
                "actions": json.loads(row["actions_json"]),
                **summary,
            })
        result.sort(
            key=lambda item: (
                Decimal(item["total_shortfall_kw"]),
                Decimal(item["tenant_impact_kw"]),
                item["strategy"],
            )
        )
        return result

    def confirm_candidate(
        self,
        actor_id: str,
        event_id: str,
        candidate_id: str,
        expected_version: int,
    ) -> dict[str, Any]:
        self._require(actor_id, "event.confirm")
        current = self._current_event(event_id)
        if int(current["version"]) != expected_version:
            raise InvalidState("指令版本已变化，请基于最新版本确认")
        if current["state"] != "planned":
            raise InvalidState("指令当前没有可确认的候选组合")
        candidate = self.connection.execute(
            "SELECT * FROM dr_candidates WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if candidate is None or candidate["event_id"] != event_id:
            raise NotFound("候选组合不存在")
        if candidate["version"] != current["version"] or candidate["state"] != "proposed":
            raise InvalidState("候选组合不是当前可确认版本")
        actions = json.loads(candidate["actions_json"])
        resource_ids = sorted({action["resource_id"] for action in actions})
        with transaction(self.connection, immediate=True):
            for resource_id in resource_ids:
                cursor = self.connection.execute(
                    "UPDATE dr_resources SET state='frozen',frozen_event_id=?,revision=revision+1 "
                    "WHERE resource_id=? AND state='available'",
                    (event_id, resource_id),
                )
                if cursor.rowcount != 1:
                    raise Conflict(f"资源 {resource_id} 已被冻结或不可用")
            for index, action in enumerate(actions):
                self.connection.execute(
                    "INSERT INTO dr_actions(action_id,event_id,phase_seq,candidate_id,resource_id,kind,"
                    "tenant_id,planned_kw,starts_at,ends_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"{candidate_id}:a{index:03d}",
                        event_id,
                        action["phase_seq"],
                        candidate_id,
                        action["resource_id"],
                        action["kind"],
                        action["tenant_id"],
                        action["planned_kw"],
                        action["starts_at"],
                        action["ends_at"],
                        self._now(),
                    ),
                )
            self.connection.execute(
                "UPDATE dr_candidates SET state='confirmed' WHERE candidate_id=?", (candidate_id,)
            )
            self.connection.execute(
                "UPDATE dr_candidates SET state='superseded' "
                "WHERE event_id=? AND version=? AND state='proposed' AND candidate_id<>?",
                (event_id, current["version"], candidate_id),
            )
            self.connection.execute(
                "UPDATE dr_events SET state='confirmed' WHERE event_id=? AND version=?",
                (event_id, current["version"]),
            )
            self._audit(
                "event",
                event_id,
                "candidate.confirmed",
                actor_id,
                {
                    "candidate_id": candidate_id,
                    "version": current["version"],
                    "actions": len(actions),
                    "frozen_resources": resource_ids,
                },
            )
        return {
            "event_id": event_id,
            "version": current["version"],
            "candidate_id": candidate_id,
            "state": "confirmed",
            "actions": len(actions),
            "frozen_resources": resource_ids,
        }

    # ------------------------------------------------------------------
    # 执行回执归并
    # ------------------------------------------------------------------



    def _refresh_phase(self, event_id: str, phase_seq: int) -> None:
        rows = self.connection.execute(
            "SELECT state FROM dr_actions WHERE event_id=? AND phase_seq=? AND state != 'cancelled'",
            (event_id, phase_seq),
        ).fetchall()
        if not rows:
            return
        states = {row["state"] for row in rows}
        if states == {"recovered"}:
            new_state = "executed"
        elif states <= {"frozen"}:
            new_state = "pending"
        else:
            new_state = "executing"
        self.connection.execute(
            "UPDATE dr_phases SET state=? WHERE event_id=? AND phase_seq=? AND state != 'superseded'",
            (new_state, event_id, phase_seq),
        )

    def record_receipt(self, actor_id: str, event_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "receipt.write")
        receipt = ReceiptInput.from_dict(raw)
        existing = self.connection.execute(
            "SELECT * FROM dr_receipts WHERE receipt_id=?", (receipt.receipt_id,)
        ).fetchone()
        if existing is not None:
            action = self.connection.execute(
                "SELECT state FROM dr_actions WHERE action_id=?", (existing["action_id"],)
            ).fetchone()
            return {
                "receipt_id": receipt.receipt_id,
                "action_id": existing["action_id"],
                "applied": bool(existing["applied"]),
                "note": existing["note"],
                "action_state": None if action is None else action["state"],
                "replayed": True,
            }
        action = self.connection.execute(
            "SELECT * FROM dr_actions WHERE action_id=? AND event_id=?",
            (receipt.action_id, event_id),
        ).fetchone()
        if action is None:
            raise NotFound("执行动作不存在")
        occurred = parse_utc(receipt.occurred_at, "occurred_at")
        if occurred > self.clock.now():
            raise ValidationFailed("回执事件时间不能晚于当前时间")
        target_state, rank = RECEIPT_TARGET[receipt.status]
        current_rank = ACTION_RANK[action["state"]]
        last_event_at = None if action["last_event_at"] is None else parse_utc(action["last_event_at"])
        applied = False
        note = ""
        if action["state"] in TERMINAL_STATES:
            note = "terminal_protected"
        elif last_event_at is not None and occurred < last_event_at:
            note = "stale_event_time"
        elif rank < current_rank:
            note = "regressive_transition"
        elif last_event_at is not None and occurred == last_event_at and rank == current_rank:
            note = "duplicate_event_time"
        else:
            applied = True
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "INSERT INTO dr_receipts(receipt_id,event_id,action_id,status,measured_kw,occurred_at,"
                "received_at,applied,note,recorded_by) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt.receipt_id,
                    event_id,
                    receipt.action_id,
                    receipt.status,
                    decimal_text(receipt.measured_kw),
                    receipt.occurred_at,
                    self._now(),
                    1 if applied else 0,
                    note,
                    actor_id,
                ),
            )
            if applied:
                actual_kw = (
                    decimal_text(receipt.measured_kw)
                    if receipt.status in MEASURED_STATUSES
                    else action["actual_kw"]
                )
                self.connection.execute(
                    "UPDATE dr_actions SET state=?,actual_kw=?,last_event_at=?,last_receipt_id=? "
                    "WHERE action_id=?",
                    (target_state, actual_kw, receipt.occurred_at, receipt.receipt_id, receipt.action_id),
                )
                if target_state in RELEASE_STATES:
                    self._release_resource_if_idle(event_id, action["resource_id"])
                self._refresh_phase(event_id, action["phase_seq"])
            self._audit(
                "receipt",
                receipt.receipt_id,
                "receipt.recorded",
                actor_id,
                {
                    "event_id": event_id,
                    "action_id": receipt.action_id,
                    "status": receipt.status,
                    "applied": applied,
                    "note": note,
                },
            )
        new_state = self.connection.execute(
            "SELECT state FROM dr_actions WHERE action_id=?", (receipt.action_id,)
        ).fetchone()["state"]
        return {
            "receipt_id": receipt.receipt_id,
            "action_id": receipt.action_id,
            "applied": applied,
            "note": note,
            "action_state": new_state,
            "replayed": False,
        }

    # ------------------------------------------------------------------
    # 关闭、查询与报表
    # ------------------------------------------------------------------

    def close_event(self, actor_id: str, event_id: str) -> dict[str, Any]:
        self._require(actor_id, "event.close")
        current = self._current_event(event_id)
        if current["state"] != "confirmed":
            raise InvalidState("只有已确认指令可以关闭")
        open_phases = self.connection.execute(
            "SELECT COUNT(*) AS count FROM dr_phases WHERE event_id=? AND state NOT IN ('executed','superseded')",
            (event_id,),
        ).fetchone()["count"]
        if open_phases:
            raise InvalidState("存在未完结阶段，不能关闭指令")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE dr_events SET state='closed' WHERE event_id=? AND version=?",
                (event_id, current["version"]),
            )
            self.connection.execute(
                "UPDATE dr_resources SET state='available',frozen_event_id=NULL,revision=revision+1 "
                "WHERE state='frozen' AND frozen_event_id=?",
                (event_id,),
            )
            self._audit("event", event_id, "event.closed", actor_id, {"version": current["version"]})
        return {"event_id": event_id, "version": current["version"], "state": "closed"}

    def event_detail(self, event_id: str) -> dict[str, Any]:
        current = self._current_event(event_id)
        versions = self.connection.execute(
            "SELECT version,state,created_by,created_at FROM dr_events WHERE event_id=? ORDER BY version",
            (event_id,),
        ).fetchall()
        candidates = self.connection.execute(
            "SELECT candidate_id,version,strategy,feasible,state FROM dr_candidates "
            "WHERE event_id=? ORDER BY candidate_id",
            (event_id,),
        ).fetchall()
        actions = self.connection.execute(
            "SELECT action_id,phase_seq,resource_id,kind,tenant_id,planned_kw,actual_kw,state,"
            "last_event_at,last_receipt_id FROM dr_actions WHERE event_id=? ORDER BY action_id",
            (event_id,),
        ).fetchall()
        return {
            "event_id": event_id,
            "site_id": current["site_id"],
            "version": current["version"],
            "state": current["state"],
            "window_start": current["window_start"],
            "window_end": current["window_end"],
            "recovery_minutes": current["recovery_minutes"],
            "baseline_kw": current["baseline_kw"],
            "curve": json.loads(current["curve_json"]),
            "versions": [dict(row) for row in versions],
            "phases": [dict(row) for row in self._phases(event_id)],
            "candidates": [
                {**dict(row), "feasible": bool(row["feasible"])} for row in candidates
            ],
            "actions": [dict(row) for row in actions],
        }

    def event_report(self, actor_id: str, event_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        current = self._current_event(event_id)
        now = self.clock.now()
        phases = [phase for phase in self._phases(event_id) if phase["state"] != "superseded"]
        action_rows = self.connection.execute(
            "SELECT * FROM dr_actions WHERE event_id=? AND state != 'cancelled' ORDER BY action_id",
            (event_id,),
        ).fetchall()
        by_phase: dict[int, list[sqlite3.Row]] = {}
        for action in action_rows:
            by_phase.setdefault(action["phase_seq"], []).append(action)
        resources = {
            row["resource_id"]: row
            for row in self.connection.execute(
                "SELECT * FROM dr_resources WHERE site_id=?", (current["site_id"],)
            ).fetchall()
        }
        floors = {
            row["tenant_id"]: row
            for row in self.connection.execute(
                "SELECT * FROM dr_tenant_floors WHERE site_id=?", (current["site_id"],)
            ).fetchall()
        }
        phase_reports: list[dict[str, Any]] = []
        reasons_all: set[str] = set()
        total_target = ZERO
        total_planned = ZERO
        total_actual = ZERO
        for phase in phases:
            target = Decimal(phase["target_reduction_kw"])
            phase_actions = by_phase.get(phase["phase_seq"], [])
            planned = sum((Decimal(action["planned_kw"]) for action in phase_actions), ZERO)
            actual = sum((Decimal(action["actual_kw"]) for action in phase_actions), ZERO)
            receipts_applied = any(action["last_event_at"] is not None for action in phase_actions)
            any_failed = any(action["state"] == "failed" for action in phase_actions)
            ended = parse_utc(phase["ends_at"]) <= now
            started = parse_utc(phase["starts_at"]) <= now or receipts_applied
            reasons = shortfall_reasons(
                target_kw=target,
                planned_kw=planned,
                actual_kw=actual,
                receipts_applied=receipts_applied,
                any_failed=any_failed,
                phase_started=started,
                phase_ended=ended,
            )
            reasons_all.update(reasons)
            total_target += target
            total_planned += planned
            total_actual += actual
            percent = achievement_percent(actual, target) if receipts_applied or ended else None
            phase_reports.append({
                "phase_seq": phase["phase_seq"],
                "state": phase["state"],
                "starts_at": phase["starts_at"],
                "ends_at": phase["ends_at"],
                "target_reduction_kw": decimal_text(target),
                "planned_kw": decimal_text(quantize_kw(planned)),
                "actual_kw": decimal_text(quantize_kw(actual)),
                "planned_shortfall_kw": decimal_text(quantize_kw(max(target - planned, ZERO))),
                "actual_shortfall_kw": (
                    decimal_text(quantize_kw(max(target - actual, ZERO)))
                    if receipts_applied or ended
                    else None
                ),
                "achievement_percent": None if percent is None else decimal_text(percent),
                "shortfall_reasons": reasons,
            })
        tenant_buckets: dict[str, dict[str, Decimal]] = {}
        delayed_minutes: dict[str, Decimal] = {}
        for action in action_rows:
            if action["tenant_id"] is None:
                continue
            bucket = tenant_buckets.setdefault(action["tenant_id"], {"planned": ZERO, "actual": ZERO})
            bucket["planned"] += Decimal(action["planned_kw"])
            bucket["actual"] += Decimal(action["actual_kw"])
            if action["kind"] == "job_delay":
                minutes = Decimal(
                    str((parse_utc(action["ends_at"]) - parse_utc(action["starts_at"])).total_seconds())
                ) / Decimal(60)
                delayed_minutes[action["tenant_id"]] = delayed_minutes.get(action["tenant_id"], ZERO) + minutes
        tenant_impacts: list[dict[str, Any]] = []
        for tenant_id in sorted(tenant_buckets):
            bucket = tenant_buckets[tenant_id]
            floor = floors.get(tenant_id)
            current_load = ZERO if floor is None else Decimal(floor["current_load_kw"])
            min_capacity = ZERO if floor is None else Decimal(floor["min_capacity_kw"])
            delivered = current_load - bucket["actual"]
            tenant_impacts.append({
                "tenant_id": tenant_id,
                "planned_reduction_kw": decimal_text(quantize_kw(bucket["planned"])),
                "actual_reduction_kw": decimal_text(quantize_kw(bucket["actual"])),
                "delayed_minutes": decimal_text(quantize_kw(delayed_minutes.get(tenant_id, ZERO))),
                "current_load_kw": decimal_text(current_load),
                "min_capacity_kw": decimal_text(min_capacity),
                "delivered_load_kw": decimal_text(quantize_kw(delivered)),
                "floor_breached": delivered < min_capacity,
            })
        recovery_plan: list[dict[str, Any]] = []
        for action in sorted(action_rows, key=lambda row: (row["ends_at"], row["resource_id"])):
            resource = resources.get(action["resource_id"])
            ramp = Decimal(resource["ramp_kw_per_minute"]) if resource is not None else Decimal("1")
            planned_kw = Decimal(action["planned_kw"])
            ramp_down = recovery_minutes_needed(planned_kw, ramp)
            eta = parse_utc(action["ends_at"]) + timedelta(minutes=ramp_down)
            recovery_plan.append({
                "action_id": action["action_id"],
                "resource_id": action["resource_id"],
                "kind": action["kind"],
                "phase_seq": action["phase_seq"],
                "planned_kw": action["planned_kw"],
                "ramp_kw_per_minute": decimal_text(ramp),
                "recovery_start": action["ends_at"],
                "ramp_down_minutes": ramp_down,
                "recovery_eta": utc_text(eta),
                "within_recovery_window": ramp_down <= int(current["recovery_minutes"]),
                "state": action["state"],
            })
        baseline = Decimal(current["baseline_kw"])
        total_percent = achievement_percent(total_actual, total_target)
        return {
            "event_id": event_id,
            "site_id": current["site_id"],
            "version": current["version"],
            "state": current["state"],
            "window_start": current["window_start"],
            "window_end": current["window_end"],
            "recovery_minutes": current["recovery_minutes"],
            "baseline_kw": decimal_text(baseline),
            "expected_load_kw": decimal_text(quantize_kw(baseline - total_target)),
            "achieved_load_kw": decimal_text(quantize_kw(baseline - total_actual)),
            "totals": {
                "target_reduction_kw": decimal_text(quantize_kw(total_target)),
                "planned_kw": decimal_text(quantize_kw(total_planned)),
                "actual_kw": decimal_text(quantize_kw(total_actual)),
                "planned_shortfall_kw": decimal_text(quantize_kw(max(total_target - total_planned, ZERO))),
                "actual_shortfall_kw": decimal_text(quantize_kw(max(total_target - total_actual, ZERO))),
                "achievement_percent": None if total_percent is None else decimal_text(total_percent),
            },
            "shortfall_reasons": sorted(reasons_all),
            "phases": phase_reports,
            "tenant_impacts": tenant_impacts,
            "recovery_plan": recovery_plan,
        }

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM grid_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
