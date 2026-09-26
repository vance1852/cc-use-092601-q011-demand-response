"""确定性的需求响应候选组合规划。

候选组合按 merit order 逐阶段贪心生成：先延迟可延迟作业，再做租户功率
封顶，最后释放储能；全程执行三类硬约束：

1. 租户保护：不可中断训练不参与，租户剩余计算功率不得低于保底值；
2. 设备爬坡：功率封顶与储能放电在相邻阶段间的新增量受爬坡率限制；
3. 恢复窗口：储能放电量必须能在 recover_by_at 前充回，作业必须能在
   承诺窗口结束时恢复。

作业延迟与封顶一旦生效即在后续阶段持续沿用（同功率削减），储能放电
同样持续，但受剩余电量与恢复充电预算约束。所有函数均为纯函数，功率与
电量统一使用 Decimal 并量化到 0.001。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from .clock import parse_utc


ZERO = Decimal("0")
QUANT = Decimal("0.001")
HOUR = Decimal(3600)


def q(value: Decimal) -> Decimal:
    return value.quantize(QUANT, rounding=ROUND_HALF_UP)


def text(value: Decimal) -> str:
    return format(value, "f")


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


REASON_MESSAGES = {
    "protected_job": "不可中断训练受保护，不允许延迟",
    "resume_window": "作业最晚恢复时间早于承诺窗口结束，无法全程延迟",
    "tenant_guarantee": "继续削减将突破租户最低算力保底",
    "ramp": "设备爬坡率限制了阶段内新增削减量",
    "storage_energy": "储能可放电量已耗尽",
    "recovery_window": "放电量超出恢复窗口内可充回的电能",
}


@dataclass(frozen=True, slots=True)
class Stage:
    stage_id: str
    starts_at: str
    ends_at: str
    target_reduction_kw: Decimal

    @property
    def hours(self) -> Decimal:
        delta = parse_utc(self.ends_at) - parse_utc(self.starts_at)
        return Decimal(delta.total_seconds()) / HOUR


@dataclass(slots=True)
class _Job:
    resource_id: str
    site_id: str
    tenant_id: str
    load_kw: Decimal
    stages: frozenset[str]
    interruptible: bool
    resume_by_at: str
    duration_minutes: Decimal
    delayed: bool = False
    delayed_stage: str | None = None


@dataclass(slots=True)
class _Cap:
    resource_id: str
    site_id: str
    tenant_id: str
    current_kw: Decimal
    max_reduction_kw: Decimal
    ramp_kw_per_stage: Decimal
    reduction: Decimal = ZERO
    engaged_stage: str | None = None


@dataclass(slots=True)
class _Storage:
    resource_id: str
    site_id: str
    available_kwh: Decimal
    discharge_kw: Decimal
    ramp_kw_per_stage: Decimal
    recharge_kw: Decimal
    reduction: Decimal = ZERO
    engaged_stage: str | None = None
    discharged_kwh: Decimal = ZERO
    energy_budget_kwh: Decimal = ZERO
    recovery_budget_kwh: Decimal = ZERO


def _build_resources(raw_resources: Sequence[Mapping[str, Any]]):
    jobs: list[_Job] = []
    caps: list[_Cap] = []
    storages: list[_Storage] = []
    for row in raw_resources:
        detail = row["detail"]
        kind = row["kind"]
        if kind == "job":
            jobs.append(
                _Job(
                    resource_id=row["resource_id"],
                    site_id=row["site_id"],
                    tenant_id=row["tenant_id"],
                    load_kw=Decimal(str(detail["load_kw"])),
                    stages=frozenset(detail["stages"]),
                    interruptible=bool(detail["interruptible"]),
                    resume_by_at=detail["resume_by_at"],
                    duration_minutes=Decimal(str(detail["duration_minutes"])),
                )
            )
        elif kind == "cap":
            caps.append(
                _Cap(
                    resource_id=row["resource_id"],
                    site_id=row["site_id"],
                    tenant_id=row["tenant_id"],
                    current_kw=Decimal(str(detail["current_kw"])),
                    max_reduction_kw=Decimal(str(detail["max_reduction_kw"])),
                    ramp_kw_per_stage=Decimal(str(detail["ramp_kw_per_stage"])),
                )
            )
        else:
            storages.append(
                _Storage(
                    resource_id=row["resource_id"],
                    site_id=row["site_id"],
                    available_kwh=Decimal(str(detail["available_kwh"])),
                    discharge_kw=Decimal(str(detail["discharge_kw"])),
                    ramp_kw_per_stage=Decimal(str(detail["ramp_kw_per_stage"])),
                    recharge_kw=Decimal(str(detail["recharge_kw"])),
                )
            )
    return jobs, caps, storages


def _site_targets(target_total: Decimal, baselines: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """按站点基线占比把全网曲线目标分摊到站点，末位吸收舍入残差。"""
    total = sum(baselines.values(), ZERO)
    if total <= ZERO:
        return {site_id: ZERO for site_id in baselines}
    ordered = sorted(baselines)
    result: dict[str, Decimal] = {}
    allocated = ZERO
    for site_id in ordered[:-1]:
        share = q(target_total * baselines[site_id] / total)
        result[site_id] = share
        allocated += share
    result[ordered[-1]] = q(target_total - allocated)
    return result


def build_plan(
    *,
    directive_id: str,
    source_revision: str,
    window_end: str,
    recover_by_at: str,
    stages_raw: Sequence[Mapping[str, Any]],
    baselines: Mapping[tuple[str, str], Decimal],
    guarantees: Mapping[tuple[str, str], Decimal],
    raw_resources: Sequence[Mapping[str, Any]],
    jobs_initial: frozenset[str] | None = None,
    caps_initial: Mapping[str, Decimal] | None = None,
    storage_initial: Mapping[str, Mapping[str, Decimal]] | None = None,
) -> dict[str, Any]:
    """生成需求响应候选组合。纯函数，输出可直接序列化与哈希。

    jobs_initial/caps_initial/storage_initial 承载已锁定阶段末尾的执行
    状态：指令修订只重排未锁阶段时，已延迟作业、功率封顶水平与储能已放
    电量必须沿续，爬坡与电量预算不得重新起算。
    """
    jobs_initial = frozenset(jobs_initial or ())
    caps_initial = dict(caps_initial or {})
    storage_initial = dict(storage_initial or {})
    stages = [
        Stage(
            stage_id=item["stage_id"],
            starts_at=item["starts_at"],
            ends_at=item["ends_at"],
            target_reduction_kw=Decimal(str(item["target_reduction_kw"])),
        )
        for item in stages_raw
    ]
    jobs, caps, storages = _build_resources(raw_resources)
    recovery_hours = Decimal(
        (parse_utc(recover_by_at) - parse_utc(window_end)).total_seconds()
    ) / HOUR

    prior_marker = "__locked_prior_stage__"
    for job in jobs:
        if job.resource_id in jobs_initial:
            job.delayed = True
            job.delayed_stage = prior_marker
    for cap in caps:
        initial = caps_initial.get(cap.resource_id)
        if initial is not None and initial > ZERO:
            cap.reduction = q(min(initial, cap.max_reduction_kw))
            cap.engaged_stage = prior_marker
    for storage in storages:
        prior = storage_initial.get(storage.resource_id)
        storage.energy_budget_kwh = q(storage.available_kwh)
        storage.recovery_budget_kwh = q(storage.recharge_kw * recovery_hours)
        if prior:
            discharged = q(prior.get("discharged_kwh", ZERO))
            initial_power = q(prior.get("reduction_kw", ZERO))
            storage.discharged_kwh = discharged
            storage.energy_budget_kwh = q(storage.energy_budget_kwh - discharged)
            storage.recovery_budget_kwh = q(storage.recovery_budget_kwh - discharged)
            if initial_power > ZERO:
                storage.reduction = initial_power
                storage.engaged_stage = prior_marker

    base_tenant_load: dict[tuple[str, str], Decimal] = {}
    for job in jobs:
        key = (job.site_id, job.tenant_id)
        base_tenant_load[key] = base_tenant_load.get(key, ZERO) + job.load_kw
    for cap in caps:
        key = (cap.site_id, cap.tenant_id)
        base_tenant_load[key] = base_tenant_load.get(key, ZERO) + cap.current_kw

    def effective_load(site_id: str, tenant_id: str, stage_id: str) -> Decimal:
        value = base_tenant_load.get((site_id, tenant_id), ZERO)
        for job in jobs:
            if (
                job.delayed
                and job.site_id == site_id
                and job.tenant_id == tenant_id
                and stage_id in job.stages
            ):
                value -= job.load_kw
        for cap in caps:
            if cap.site_id == site_id and cap.tenant_id == tenant_id:
                value -= cap.reduction
        return value

    def storage_stage_ceiling(storage: _Storage, stage_start: Decimal,
                              remaining_hours: Decimal,
                              energy_budget: Decimal, recovery_budget: Decimal,
                              ) -> tuple[Decimal, str | None]:
        """阶段末放电功率上限：额定、爬坡与剩余电量/恢复预算的可持续功率。"""
        energy_power = energy_budget / remaining_hours if remaining_hours > ZERO else ZERO
        recovery_power = recovery_budget / remaining_hours if remaining_hours > ZERO else ZERO
        ramp_ceiling = stage_start + storage.ramp_kw_per_stage
        ceiling = min(storage.discharge_kw, ramp_ceiling, energy_power, recovery_power)
        limits = [
            (storage.discharge_kw, None),
            (ramp_ceiling, "ramp"),
            (energy_power, "storage_energy"),
            (recovery_power, "recovery_window"),
        ]
        binding = min(limits, key=lambda item: item[0])[1]
        return q(ceiling), binding

    stage_results: list[dict[str, Any]] = []

    for stage_index, stage in enumerate(stages):
        stage_baselines = {
            site_id: value
            for (site_id, stage_id), value in baselines.items()
            if stage_id == stage.stage_id
        }
        targets = _site_targets(stage.target_reduction_kw, stage_baselines)
        remaining_window_hours = sum((item.hours for item in stages[stage_index:]), ZERO)

        # 阶段起点快照：爬坡只约束相对起点的新增量，预算判定也基于起点余额。
        cap_start = {cap.resource_id: cap.reduction for cap in caps}
        storage_start = {storage.resource_id: storage.reduction for storage in storages}
        energy_start = {storage.resource_id: storage.energy_budget_kwh for storage in storages}
        recovery_start = {storage.resource_id: storage.recovery_budget_kwh for storage in storages}
        # 储能若剩余电量已无法维持起点功率，先做物理下调。
        for storage in storages:
            energy_power = energy_start[storage.resource_id] / remaining_window_hours
            recovery_power = recovery_start[storage.resource_id] / remaining_window_hours
            floor_power = min(energy_power, recovery_power)
            if storage.reduction > floor_power:
                storage.reduction = q(floor_power)
                storage_start[storage.resource_id] = storage.reduction

        engaged_this_stage: set[tuple[str, str]] = set()
        site_planned: dict[str, Decimal] = {site_id: ZERO for site_id in targets}

        # 沿用上一阶段已生效的削减（作业延迟、封顶、储能持续放电）。
        for job in jobs:
            if job.delayed and stage.stage_id in job.stages:
                site_planned[job.site_id] = q(site_planned[job.site_id] + job.load_kw)
        for cap in caps:
            if cap.reduction > ZERO:
                site_planned[cap.site_id] = q(site_planned[cap.site_id] + cap.reduction)
                if cap.engaged_stage == stage.stage_id:
                    engaged_this_stage.add(("cap", cap.resource_id))
        for storage in storages:
            if storage.reduction > ZERO:
                site_planned[storage.site_id] = q(site_planned[storage.site_id] + storage.reduction)
                if storage.engaged_stage == stage.stage_id:
                    engaged_this_stage.add(("storage", storage.resource_id))

        def remaining(site_id: str) -> Decimal:
            return q(targets[site_id] - site_planned[site_id])

        # 1) 延迟作业：可中断、负荷大优先；整块延迟，允许略微超额完成曲线。
        job_order = sorted(
            [
                job
                for job in jobs
                if stage.stage_id in job.stages
                and job.site_id in targets
                and not job.delayed
            ],
            key=lambda job: (not job.interruptible, -job.load_kw, job.resource_id),
        )
        for job in job_order:
            if remaining(job.site_id) <= ZERO:
                break
            if not job.interruptible:
                continue
            if parse_utc(job.resume_by_at) < parse_utc(window_end):
                continue
            guarantee = guarantees.get((job.site_id, job.tenant_id), ZERO)
            if effective_load(job.site_id, job.tenant_id, stage.stage_id) - job.load_kw < guarantee:
                continue
            job.delayed = True
            job.delayed_stage = stage.stage_id
            site_planned[job.site_id] = q(site_planned[job.site_id] + job.load_kw)
            engaged_this_stage.add(("job", job.resource_id))

        # 2) 功率封顶：阶段内只做一次爬坡动作，受租户保底约束。
        cap_order = sorted(
            [cap for cap in caps if cap.site_id in targets],
            key=lambda cap: (cap.site_id, cap.tenant_id, -cap.max_reduction_kw, cap.resource_id),
        )
        for cap in cap_order:
            gap = remaining(cap.site_id)
            if gap <= ZERO:
                break
            start = cap_start[cap.resource_id]
            ramp_ceiling = q(min(cap.max_reduction_kw, start + cap.ramp_kw_per_stage))
            # 实时有效负荷已扣除延迟作业与所有封顶增量，保底约束即剩余可封空间。
            guarantee_room = q(
                effective_load(cap.site_id, cap.tenant_id, stage.stage_id)
                - guarantees.get((cap.site_id, cap.tenant_id), ZERO)
            )
            target_level = q(min(ramp_ceiling, start + gap, cap.reduction + max(ZERO, guarantee_room)))
            extra = q(target_level - cap.reduction)
            if extra <= ZERO:
                continue
            cap.reduction = target_level
            site_planned[cap.site_id] = q(site_planned[cap.site_id] + extra)
            if cap.engaged_stage is None:
                cap.engaged_stage = stage.stage_id
                engaged_this_stage.add(("cap", cap.resource_id))

        # 3) 储能放电：阶段内单次爬坡，叠加电量与恢复窗口预算。
        storage_order = sorted(
            [storage for storage in storages if storage.site_id in targets],
            key=lambda storage: (storage.site_id, -storage.discharge_kw, storage.resource_id),
        )
        for storage in storage_order:
            gap = remaining(storage.site_id)
            if gap <= ZERO:
                continue
            start = storage_start[storage.resource_id]
            ceiling, _ = storage_stage_ceiling(
                storage, start, remaining_window_hours,
                energy_start[storage.resource_id], recovery_start[storage.resource_id],
            )
            target_level = q(min(ceiling, start + gap))
            extra = q(target_level - storage.reduction)
            if extra <= ZERO:
                continue
            storage.reduction = target_level
            site_planned[storage.site_id] = q(site_planned[storage.site_id] + extra)
            if storage.engaged_stage is None:
                storage.engaged_stage = stage.stage_id
                engaged_this_stage.add(("storage", storage.resource_id))

        # 本阶段放电量从电量与恢复预算中扣减。
        for storage in storages:
            if storage.reduction > ZERO:
                used = q(storage.reduction * stage.hours)
                storage.discharged_kwh = q(storage.discharged_kwh + used)
                storage.energy_budget_kwh = q(storage.energy_budget_kwh - used)
                storage.recovery_budget_kwh = q(storage.recovery_budget_kwh - used)

        # 汇总每资源在本阶段的实际削减动作。
        actions: list[dict[str, Any]] = []
        for job in jobs:
            if job.delayed and stage.stage_id in job.stages:
                actions.append({
                    "site_id": job.site_id,
                    "resource_id": job.resource_id,
                    "kind": "job",
                    "tenant_id": job.tenant_id,
                    "action": "delay",
                    "reduction_kw": text(job.load_kw),
                    "engaged_this_stage": ("job", job.resource_id) in engaged_this_stage,
                })
        for cap in caps:
            if cap.reduction > ZERO:
                actions.append({
                    "site_id": cap.site_id,
                    "resource_id": cap.resource_id,
                    "kind": "cap",
                    "tenant_id": cap.tenant_id,
                    "action": "power_cap",
                    "reduction_kw": text(cap.reduction),
                    "engaged_this_stage": ("cap", cap.resource_id) in engaged_this_stage,
                })
        for storage in storages:
            if storage.reduction > ZERO:
                actions.append({
                    "site_id": storage.site_id,
                    "resource_id": storage.resource_id,
                    "kind": "storage",
                    "tenant_id": None,
                    "action": "discharge",
                    "reduction_kw": text(storage.reduction),
                    "engaged_this_stage": ("storage", storage.resource_id) in engaged_this_stage,
                })
        actions.sort(key=lambda item: (item["site_id"], item["kind"], item["resource_id"]))

        # 缺口诊断：仍有缺口时按资源归并未动用能力的首要约束。
        reasons_map: dict[str, dict[str, Any]] = {}

        def diagnose(code: str | None, blocked: Decimal, resource_id: str) -> None:
            blocked = q(blocked)
            if code is None or blocked <= ZERO:
                return
            bucket = reasons_map.setdefault(
                code,
                {"code": code, "message": REASON_MESSAGES[code], "blocked_kw": ZERO, "resource_refs": []},
            )
            bucket["blocked_kw"] = q(bucket["blocked_kw"] + blocked)
            if resource_id not in bucket["resource_refs"]:
                bucket["resource_refs"].append(resource_id)

        gap_total = ZERO
        excess_total = ZERO
        site_rows = []
        for site_id in sorted(targets):
            planned = site_planned[site_id]
            gap = q(max(ZERO, targets[site_id] - planned))
            excess = q(max(ZERO, planned - targets[site_id]))
            gap_total = q(gap_total + gap)
            excess_total = q(excess_total + excess)
            site_rows.append({
                "site_id": site_id,
                "baseline_kw": text(stage_baselines[site_id]),
                "target_kw": text(targets[site_id]),
                "planned_reduction_kw": text(planned),
                "gap_kw": text(gap),
                "excess_kw": text(excess),
            })

        if gap_total > ZERO:
            for job in job_order:
                guarantee = guarantees.get((job.site_id, job.tenant_id), ZERO)
                if not job.interruptible:
                    diagnose("protected_job", job.load_kw, job.resource_id)
                elif parse_utc(job.resume_by_at) < parse_utc(window_end):
                    diagnose("resume_window", job.load_kw, job.resource_id)
                elif effective_load(job.site_id, job.tenant_id, stage.stage_id) - job.load_kw < guarantee:
                    diagnose("tenant_guarantee", job.load_kw, job.resource_id)
            for cap in cap_order:
                start = cap_start[cap.resource_id]
                ramp_ceiling = q(min(cap.max_reduction_kw, start + cap.ramp_kw_per_stage))
                guarantee_room = q(
                    effective_load(cap.site_id, cap.tenant_id, stage.stage_id)
                    - guarantees.get((cap.site_id, cap.tenant_id), ZERO)
                )
                if guarantee_room <= ZERO:
                    diagnose("tenant_guarantee", q(cap.max_reduction_kw - cap.reduction), cap.resource_id)
                elif ramp_ceiling < cap.max_reduction_kw:
                    diagnose("ramp", q(cap.max_reduction_kw - ramp_ceiling), cap.resource_id)
            for storage in storage_order:
                start = storage_start[storage.resource_id]
                ceiling, binding = storage_stage_ceiling(
                    storage, start, remaining_window_hours,
                    energy_start[storage.resource_id], recovery_start[storage.resource_id],
                )
                blocked = q(storage.discharge_kw - ceiling)
                if blocked > ZERO:
                    diagnose(binding or "storage_energy", blocked, storage.resource_id)

        stage_results.append({
            "stage_id": stage.stage_id,
            "starts_at": stage.starts_at,
            "ends_at": stage.ends_at,
            "duration_hours": text(stage.hours),
            "target_reduction_kw": text(stage.target_reduction_kw),
            "planned_reduction_kw": text(q(sum(site_planned.values(), ZERO))),
            "gap_kw": text(gap_total),
            "excess_kw": text(excess_total),
            "met": gap_total <= ZERO,
            "sites": site_rows,
            "actions": actions,
            "reasons": [
                {
                    "code": bucket["code"],
                    "message": bucket["message"],
                    "blocked_kw": text(bucket["blocked_kw"]),
                    "resource_refs": bucket["resource_refs"],
                }
                for _, bucket in sorted(reasons_map.items())
            ],
        })

    total_target_kwh = ZERO
    total_planned_kwh = ZERO
    for stage, result in zip(stages, stage_results):
        total_target_kwh = q(total_target_kwh + stage.target_reduction_kw * stage.hours)
        total_planned_kwh = q(total_planned_kwh + Decimal(result["planned_reduction_kw"]) * stage.hours)

    # 租户影响：窗口内峰值削减功率，以及涉及的作业与封顶资源。
    peak: dict[tuple[str, str], Decimal] = {}
    delayed_refs: dict[tuple[str, str], list[str]] = {}
    capped_refs: dict[tuple[str, str], list[str]] = {}
    for result in stage_results:
        per_tenant: dict[tuple[str, str], Decimal] = {}
        for action in result["actions"]:
            if action["tenant_id"] is None:
                continue
            key = (action["site_id"], action["tenant_id"])
            per_tenant[key] = per_tenant.get(key, ZERO) + Decimal(action["reduction_kw"])
            if action["kind"] == "job":
                delayed_refs.setdefault(key, [])
                if action["resource_id"] not in delayed_refs[key]:
                    delayed_refs[key].append(action["resource_id"])
            elif action["kind"] == "cap":
                capped_refs.setdefault(key, [])
                if action["resource_id"] not in capped_refs[key]:
                    capped_refs[key].append(action["resource_id"])
        for key, value in per_tenant.items():
            peak[key] = max(peak.get(key, ZERO), value)
    impact_rows = []
    for key in sorted(set(peak) | set(delayed_refs) | set(capped_refs)):
        impact_rows.append({
            "site_id": key[0],
            "tenant_id": key[1],
            "protected_min_compute_kw": text(guarantees.get(key, ZERO)),
            "peak_reduced_kw": text(peak.get(key, ZERO)),
            "delayed_jobs": sorted(delayed_refs.get(key, [])),
            "capped_resources": sorted(capped_refs.get(key, [])),
        })

    recovery_jobs = [
        {
            "resource_id": job.resource_id,
            "site_id": job.site_id,
            "tenant_id": job.tenant_id,
            "resume_at": window_end,
            "remaining_duration_minutes": text(job.duration_minutes),
        }
        for job in sorted([item for item in jobs if item.delayed], key=lambda item: item.resource_id)
    ]
    recovery_caps = [
        {
            "resource_id": cap.resource_id,
            "site_id": cap.site_id,
            "tenant_id": cap.tenant_id,
            "release_cap_at": window_end,
            "restore_to_kw": text(cap.current_kw),
        }
        for cap in sorted([item for item in caps if item.reduction > ZERO], key=lambda item: item.resource_id)
    ]
    recovery_storage = []
    for storage in sorted(storages, key=lambda item: item.resource_id):
        if storage.discharged_kwh <= ZERO:
            continue
        recharge_hours = storage.discharged_kwh / storage.recharge_kw
        ready_seconds = int(q(recharge_hours * HOUR).to_integral_value(rounding=ROUND_HALF_UP))
        ready_at = (
            parse_utc(window_end) + timedelta(seconds=ready_seconds)
        ).isoformat().replace("+00:00", "Z")
        recovery_storage.append({
            "resource_id": storage.resource_id,
            "site_id": storage.site_id,
            "discharged_kwh": text(storage.discharged_kwh),
            "recharge_kw": text(storage.recharge_kw),
            "recharge_start_at": window_end,
            "recharge_ready_at": ready_at,
            "recover_by_at": recover_by_at,
            "within_recovery_window": parse_utc(ready_at) <= parse_utc(recover_by_at),
        })

    return {
        "directive_id": directive_id,
        "source_revision": source_revision,
        "stages": stage_results,
        "total_target_kwh": text(total_target_kwh),
        "total_planned_kwh": text(total_planned_kwh),
        "feasible": all(item["met"] for item in stage_results),
        "tenant_impact": impact_rows,
        "recovery_plan": {
            "window_end": window_end,
            "recover_by_at": recover_by_at,
            "jobs": recovery_jobs,
            "caps": recovery_caps,
            "storage": recovery_storage,
        },
    }
