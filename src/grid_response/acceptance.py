"""贯通指令登记、候选组合、资源冻结、回执归并、修订与报表的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import GridResponseService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    clock = FrozenClock(datetime(2026, 9, 26, 13, 0, tzinfo=timezone.utc))
    service = GridResponseService(connection, clock)
    for user_id, role in (("plan", "planner"), ("ops", "operator"), ("disp", "dispatcher"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)
    service.register_site("plan", {"site_id": "site-east", "name": "东部算力园区", "timezone": "Asia/Shanghai", "baseline_kw": "12000"})
    service.register_tenant_floor("plan", "site-east", {"tenant_id": "tenant-a", "current_load_kw": "5000", "min_capacity_kw": "3000"})
    service.register_tenant_floor("plan", "site-east", {"tenant_id": "tenant-b", "current_load_kw": "4000", "min_capacity_kw": "2000"})
    service.register_resource("plan", "site-east", {"resource_id": "storage-1", "kind": "storage", "adjustable_kw": "2000", "ramp_kw_per_minute": "100", "energy_kwh": "6000"})
    service.register_resource("plan", "site-east", {"resource_id": "cap-a", "kind": "power_cap", "tenant_id": "tenant-a", "adjustable_kw": "1200", "ramp_kw_per_minute": "20"})
    service.register_resource("plan", "site-east", {"resource_id": "delay-b", "kind": "job_delay", "tenant_id": "tenant-b", "adjustable_kw": "800", "ramp_kw_per_minute": "40", "max_delay_minutes": 60})
    service.register_resource("plan", "site-east", {"resource_id": "train-a", "kind": "job_delay", "tenant_id": "tenant-a", "adjustable_kw": "2000", "ramp_kw_per_minute": "50", "max_delay_minutes": 30, "protected": True})
    service.register_event("plan", {
        "event_id": "DR-001",
        "site_id": "site-east",
        "window_start": "2026-09-26T14:00:00Z",
        "window_end": "2026-09-26T16:00:00Z",
        "recovery_minutes": 30,
        "curve": [
            {"starts_at": "2026-09-26T14:00:00Z", "ends_at": "2026-09-26T15:00:00Z", "target_reduction_kw": "2500"},
            {"starts_at": "2026-09-26T15:00:00Z", "ends_at": "2026-09-26T16:00:00Z", "target_reduction_kw": "2800"},
        ],
    })
    first_run = service.generate_candidates("plan", "DR-001")
    replayed_run = service.generate_candidates("plan", "DR-001")
    candidate_v1 = next(item for item in first_run["candidates"] if item["strategy"] == "storage_first")
    service.confirm_candidate("ops", "DR-001", candidate_v1["candidate_id"], 1)
    actions_v1 = {action["resource_id"] + ":" + str(action["phase_seq"]): action for action in service.event_detail("DR-001")["actions"]}
    clock.advance(minutes=70)  # 14:10，第一阶段执行中
    receipts = {}
    receipts["achieved_first"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-002", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "achieved", "measured_kw": "1666.666", "occurred_at": "2026-09-26T14:05:00Z"})
    receipts["stale_start"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-001", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "started", "measured_kw": "0", "occurred_at": "2026-09-26T14:01:00Z"})
    receipts["duplicate"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-002", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "achieved", "measured_kw": "1666.666", "occurred_at": "2026-09-26T14:05:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-003", "action_id": actions_v1["cap-a:0"]["action_id"], "status": "achieved", "measured_kw": "600", "occurred_at": "2026-09-26T14:06:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-004", "action_id": actions_v1["delay-b:0"]["action_id"], "status": "partial", "measured_kw": "200", "occurred_at": "2026-09-26T14:06:00Z"})
    clock.advance(minutes=40)  # 14:50，电网修订剩余阶段目标
    service.register_resource("plan", "site-east", {"resource_id": "storage-2", "kind": "storage", "adjustable_kw": "1500", "ramp_kw_per_minute": "100", "energy_kwh": "4500"})
    service.register_resource("plan", "site-east", {"resource_id": "cap-b", "kind": "power_cap", "tenant_id": "tenant-b", "adjustable_kw": "1000", "ramp_kw_per_minute": "50"})
    revision = service.revise_event("plan", "DR-001", {"curve": [{"starts_at": "2026-09-26T15:00:00Z", "ends_at": "2026-09-26T16:00:00Z", "target_reduction_kw": "2000"}]})
    second_run = service.generate_candidates("plan", "DR-001")
    candidate_v2 = next(item for item in second_run["candidates"] if item["strategy"] == "storage_first")
    service.confirm_candidate("ops", "DR-001", candidate_v2["candidate_id"], 2)
    detail = service.event_detail("DR-001")
    actions_v2 = {action["resource_id"]: action for action in detail["actions"] if action["phase_seq"] == 2}
    clock.advance(minutes=40)  # 15:30，第一阶段进入恢复，回执继续乱序到达
    receipts["recovered_first"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-006", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T15:20:00Z"})
    receipts["late_recovery"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-005", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "recovery_started", "measured_kw": "0", "occurred_at": "2026-09-26T15:00:00Z"})
    receipts["terminal_protected"] = service.record_receipt("disp", "DR-001", {"receipt_id": "rc-007", "action_id": actions_v1["storage-1:0"]["action_id"], "status": "started", "measured_kw": "0", "occurred_at": "2026-09-26T15:25:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-008", "action_id": actions_v1["cap-a:0"]["action_id"], "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T15:21:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-009", "action_id": actions_v1["delay-b:0"]["action_id"], "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T15:22:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-101", "action_id": actions_v2["storage-2"]["action_id"], "status": "achieved", "measured_kw": "1312.500", "occurred_at": "2026-09-26T15:05:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-102", "action_id": actions_v2["cap-b"]["action_id"], "status": "achieved", "measured_kw": "687.500", "occurred_at": "2026-09-26T15:06:00Z"})
    clock.advance(minutes=70)  # 16:40，第二阶段恢复完成
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-103", "action_id": actions_v2["storage-2"]["action_id"], "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T16:15:00Z"})
    service.record_receipt("disp", "DR-001", {"receipt_id": "rc-104", "action_id": actions_v2["cap-b"]["action_id"], "status": "recovered", "measured_kw": "0", "occurred_at": "2026-09-26T16:18:00Z"})
    closed = service.close_event("ops", "DR-001")
    report = service.event_report("audit", "DR-001")
    final_actions = service.event_detail("DR-001")["actions"]
    final_resources = [
        service.resource_detail(resource_id)["state"]
        for resource_id in ("storage-1", "cap-a", "delay-b", "storage-2", "cap-b")
    ]
    result = {
        "status": "ok",
        "workspace": workspace.name,
        "event_id": "DR-001",
        "version": revision["version"],
        "state": closed["state"],
        "first_run_replayed": replayed_run["replayed"],
        "receipt_notes": {key: value["note"] for key, value in receipts.items()},
        "receipt_replayed": receipts["duplicate"]["replayed"],
        "phases": {phase["phase_seq"]: phase["state"] for phase in report["phases"]},
        "totals": report["totals"],
        "shortfall_reasons": report["shortfall_reasons"],
        "tenant_impacts": report["tenant_impacts"],
        "recovery_within_window": all(item["within_recovery_window"] for item in report["recovery_plan"]),
        "resources_released": all(
            action["state"] in ("recovered", "cancelled") for action in final_actions
        ) and all(state == "available" for state in final_resources),
        "audit": service.audit_chain("audit"),
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行电网需求响应协同服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
