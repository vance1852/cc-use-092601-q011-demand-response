"""需求响应协同模块离线验收：登记、候选、冻结、乱序回执与修订归并。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import DemandResponseService


def _directive(target_stage2: str = "450") -> dict[str, object]:
    return {
        "directive_id": "dr-20260925-01",
        "source_revision": "grid-rev-1",
        "window_start": "2026-09-25T13:00:00Z",
        "window_end": "2026-09-25T15:00:00Z",
        "recover_by_at": "2026-09-25T16:00:00Z",
        "note": "晚峰需求响应 450/480 千瓦",
        "curve": [
            {"stage_id": "stage-1", "starts_at": "2026-09-25T13:00:00Z",
             "ends_at": "2026-09-25T14:00:00Z", "target_reduction_kw": "300"},
            {"stage_id": "stage-2", "starts_at": "2026-09-25T14:00:00Z",
             "ends_at": "2026-09-25T15:00:00Z", "target_reduction_kw": target_stage2},
        ],
        "baselines": [
            {"site_id": "site-a", "stage_id": "stage-1", "baseline_kw": "1000"},
            {"site_id": "site-a", "stage_id": "stage-2", "baseline_kw": "1000"},
            {"site_id": "site-b", "stage_id": "stage-1", "baseline_kw": "500"},
            {"site_id": "site-b", "stage_id": "stage-2", "baseline_kw": "500"},
        ],
        "guarantees": [
            {"site_id": "site-a", "tenant_id": "tenant-1", "min_compute_kw": "200"},
        ],
    }


RESOURCES = [
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "job-train-1", "site_id": "site-a", "kind": "job",
        "detail": {"tenant_id": "tenant-1", "load_kw": "120", "stages": ["stage-1", "stage-2"],
                   "interruptible": True, "resume_by_at": "2026-09-25T16:00:00Z",
                   "duration_minutes": "60"},
    },
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "job-train-protected", "site_id": "site-a", "kind": "job",
        "detail": {"tenant_id": "tenant-1", "load_kw": "80", "stages": ["stage-1", "stage-2"],
                   "interruptible": False, "resume_by_at": "2026-09-25T16:00:00Z",
                   "duration_minutes": "120"},
    },
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "cap-a1", "site_id": "site-a", "kind": "cap",
        "detail": {"tenant_id": "tenant-1", "current_kw": "300", "max_reduction_kw": "150",
                   "ramp_kw_per_stage": "100", "floor_kw": "150"},
    },
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "storage-a2", "site_id": "site-a", "kind": "storage",
        "detail": {"available_kwh": "200", "discharge_kw": "80", "ramp_kw_per_stage": "80",
                   "recharge_kw": "100"},
    },
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "job-train-2", "site_id": "site-b", "kind": "job",
        "detail": {"tenant_id": "tenant-2", "load_kw": "60", "stages": ["stage-1", "stage-2"],
                   "interruptible": True, "resume_by_at": "2026-09-25T16:00:00Z",
                   "duration_minutes": "45"},
    },
    {
        "directive_id": "dr-20260925-01",
        "resource_id": "storage-b1", "site_id": "site-b", "kind": "storage",
        "detail": {"available_kwh": "300", "discharge_kw": "200", "ramp_kw_per_stage": "100",
                   "recharge_kw": "150"},
    },
]


def _receipt(event_id: str, stage: str, resource: str, event_type: str, event_time: str,
             observed: str | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "directive_id": "dr-20260925-01",
        "source_event_id": event_id,
        "stage_id": stage,
        "resource_id": resource,
        "event_type": event_type,
        "event_time": event_time,
    }
    if observed is not None:
        payload["observed_reduction_kw"] = observed
    return payload


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = DemandResponseService(
        connection, FrozenClock(datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc))
    )
    for user_id, role in (("ops", "operator"), ("lead", "approver"), ("audit", "auditor")):
        service.create_user(user_id, user_id, role)

    # 1) 登记指令版本与站点基线、租户保底。
    directive = _directive()
    directive["idempotency_key"] = "dr-reg-1"
    registered = service.register_directive("ops", directive)
    assert registered["version"] == 1
    replayed = service.register_directive("ops", directive)
    assert replayed == registered

    # 2) 登记资源可调属性（含不可中断训练）。
    for resource in RESOURCES:
        service.register_resource("ops", resource)

    # 3) 生成候选组合（确定性重放）。
    candidate = service.generate_candidates("ops", "dr-20260925-01")
    assert candidate["feasible"] is True, candidate["plan"]
    candidate_replay = service.generate_candidates("ops", "dr-20260925-01")
    assert candidate_replay["replayed"] is True
    plan = candidate["plan"]
    stage1 = next(item for item in plan["stages"] if item["stage_id"] == "stage-1")
    assert next(a for a in stage1["actions"] if a["resource_id"] == "job-train-1")
    assert all(a["resource_id"] != "job-train-protected" for a in stage1["actions"])

    # 4) 运营确认，一次性冻结。
    frozen = service.confirm_candidate("lead", candidate["candidate_id"])
    assert frozen["frozen_actions"] >= 8

    # 5) stage-1 回执：先到 dispatched，再到 achieved；乱序与终态必须被归并/保护。
    service.ingest_receipt("ops", _receipt("evt-1", "stage-1", "job-train-1", "dispatched", "2026-09-25T13:02:00Z"))
    stale = service.ingest_receipt("ops", _receipt("evt-3", "stage-1", "job-train-1", "achieved", "2026-09-25T13:01:00Z", "120"))
    assert stale["applied"] is False and stale["ignored_reason"] == "stale_event_time"
    service.ingest_receipt("ops", _receipt("evt-2", "stage-1", "job-train-1", "achieved", "2026-09-25T13:05:00Z", "120"))
    late_dispatch = service.ingest_receipt("ops", _receipt("evt-5", "stage-1", "job-train-1", "dispatched", "2026-09-25T13:04:00Z"))
    assert late_dispatch["applied"] is False and late_dispatch["ignored_reason"] == "terminal_state_protected"
    terminal = service.ingest_receipt("ops", _receipt("evt-4", "stage-1", "job-train-1", "failed", "2026-09-25T13:06:00Z"))
    assert terminal["applied"] is False and terminal["ignored_reason"] == "terminal_state_protected"
    duplicate = service.ingest_receipt("ops", _receipt("evt-2", "stage-1", "job-train-1", "achieved", "2026-09-25T13:05:00Z", "120"))
    assert duplicate["duplicate"] is True and duplicate["applied"] is True
    for resource, observed in (
        ("cap-a1", "80"),
        ("job-train-2", "60"),
        ("storage-b1", "40"),
    ):
        service.ingest_receipt("ops", _receipt(f"d-{resource}", "stage-1", resource, "dispatched", "2026-09-25T13:02:00Z"))
        service.ingest_receipt("ops", _receipt(f"a-{resource}", "stage-1", resource, "achieved", "2026-09-25T13:05:00Z", observed))

    view = service.directive_view("audit", "dr-20260925-01")
    assert view["locked_stages"] == ["stage-1"]

    # 6) 电网修订：stage-2 目标上调到 480；stage-1 已锁，曲线必须原样保留。
    revised_payload = _directive("480")
    revised_payload["source_revision"] = "grid-rev-2"
    revised_payload["idempotency_key"] = "dr-rev-1"
    revised = service.revise_directive("ops", revised_payload)
    assert revised["version"] == 2 and revised["locked_stages"] == ["stage-1"]
    candidate2 = service.generate_candidates("ops", "dr-20260925-01")
    assert candidate2["version"] == 2 and candidate2["feasible"] is True, candidate2["plan"]
    frozen2 = service.confirm_candidate("lead", candidate2["candidate_id"])
    assert frozen2["state"] == "frozen"

    # 7) stage-2 回执：储能动作失败造成未达标，封顶足额，其余 restored。
    service.ingest_receipt("ops", _receipt("e2-job1", "stage-2", "job-train-1", "achieved", "2026-09-25T14:05:00Z", "120"))
    service.ingest_receipt("ops", _receipt("e2-cap", "stage-2", "cap-a1", "achieved", "2026-09-25T14:05:00Z", "150"))
    service.ingest_receipt("ops", _receipt("e2-job2", "stage-2", "job-train-2", "achieved", "2026-09-25T14:05:00Z", "60"))
    failed_storage = service.ingest_receipt("ops", _receipt("e2-sb-fail", "stage-2", "storage-b1", "failed", "2026-09-25T14:06:00Z"))
    assert failed_storage["applied"] is True
    service.ingest_receipt("ops", _receipt("e2-sa-fail", "stage-2", "storage-a2", "failed", "2026-09-25T14:06:00Z"))
    # failed 是终态，恢复事件仍允许收敛到 restored（storage-b1 验证收敛，storage-a2 保留失败终态）。
    for event_id, resource, observed in (
        ("r-job1", "job-train-1", "120"),
        ("r-cap", "cap-a1", "150"),
        ("r-job2", "job-train-2", "60"),
        ("r-sb", "storage-b1", "0"),
    ):
        service.ingest_receipt("ops", _receipt(event_id, "stage-2", resource, "restored", "2026-09-25T15:05:00Z", observed))
    for resource in ("job-train-1", "cap-a1", "job-train-2", "storage-b1"):
        service.ingest_receipt("ops", _receipt(f"r1-{resource}", "stage-1", resource, "restored", "2026-09-25T14:05:00Z"))

    closed = service.close_directive("lead", "dr-20260925-01")
    assert closed["state"] == "closed"

    # 8) 解释查询：实际削减、未达标原因、租户影响、恢复计划。
    report = service.directive_report("audit", "dr-20260925-01")
    stage2_report = next(item for item in report["stages"] if item["stage_id"] == "stage-2")
    assert stage2_report["met"] is False
    reason_codes = {item["code"] for item in stage2_report["shortfall_reasons"]}
    assert "action_failed" in reason_codes
    tenants = {item["tenant_id"] for item in report["tenant_impact"]}
    assert tenants == {"tenant-1", "tenant-2"}
    storage_recovery = next(
        item for item in report["recovery_plan"]["storage"] if item["resource_id"] == "storage-b1"
    )
    assert storage_recovery["recharge_ready_at"] <= "2026-09-25T16:00:00Z"
    assert report["totals"]["actual_kwh"] is not None

    audit = service.audit_chain("audit")
    assert audit["valid"] is True and audit["events"] > 10

    return {
        "status": "ok",
        "registered": registered,
        "revised": revised,
        "frozen_actions": frozen["frozen_actions"] + frozen2["frozen_actions"],
        "stage2": stage2_report,
        "totals": report["totals"],
        "tenant_impact": report["tenant_impact"],
        "recovery_plan": report["recovery_plan"],
        "audit": audit,
        "workspace": workspace.name,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行需求响应协同服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
