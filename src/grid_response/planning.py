"""需求响应候选组合的确定性生成与报表计算。"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP
from typing import Mapping, Sequence

from .clock import utc_text


ZERO = Decimal("0")
HUNDRED = Decimal("100")
KW_QUANT = Decimal("0.001")
MINUTES_PER_HOUR = Decimal("60")

# 候选策略：按资源类别的取用顺序区分，保证同一输入生成同一组候选。
STRATEGIES: Mapping[str, tuple[str, ...]] = {
    "storage_first": ("storage", "power_cap", "job_delay"),
    "cap_first": ("power_cap", "storage", "job_delay"),
    "job_first": ("job_delay", "storage", "power_cap"),
}


def quantize_kw(value: Decimal) -> Decimal:
    return value.quantize(KW_QUANT, rounding=ROUND_HALF_UP)


def floor_kw(value: Decimal) -> Decimal:
    return value.quantize(KW_QUANT, rounding=ROUND_DOWN)


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class PhaseInput:
    phase_seq: int
    starts_at: datetime
    ends_at: datetime
    target_reduction_kw: Decimal
    is_final: bool

    @property
    def minutes(self) -> Decimal:
        seconds = Decimal(str((self.ends_at - self.starts_at).total_seconds()))
        return seconds / MINUTES_PER_HOUR


@dataclass(frozen=True, slots=True)
class ResourceInput:
    resource_id: str
    kind: str
    tenant_id: str | None
    adjustable_kw: Decimal
    ramp_kw_per_minute: Decimal
    max_delay_minutes: int | None
    energy_kwh: Decimal | None
    protected: bool


@dataclass(frozen=True, slots=True)
class TenantFloorInput:
    tenant_id: str
    current_load_kw: Decimal
    min_capacity_kw: Decimal


def ramp_limited_average(
    adjustable_kw: Decimal,
    ramp_kw_per_minute: Decimal,
    interval_minutes: Decimal,
) -> Decimal:
    """线性爬坡后保持的区间平均可调功率。"""
    if adjustable_kw <= ZERO or interval_minutes <= ZERO:
        return ZERO
    ramp_minutes = adjustable_kw / ramp_kw_per_minute
    if ramp_minutes >= interval_minutes:
        return ramp_kw_per_minute * interval_minutes / 2
    return adjustable_kw * (1 - ramp_minutes / (2 * interval_minutes))


def _effective_kw(
    resource: ResourceInput,
    phase: PhaseInput,
    remaining_energy_kwh: Decimal,
    tenant_headroom_kw: Decimal | None,
    recovery_minutes: int,
) -> Decimal:
    minutes = phase.minutes
    average = ramp_limited_average(resource.adjustable_kw, resource.ramp_kw_per_minute, minutes)
    if resource.kind == "job_delay":
        coverage = min(Decimal(1), Decimal(resource.max_delay_minutes or 0) / minutes)
        average *= coverage
    elif resource.kind == "storage":
        if remaining_energy_kwh <= ZERO:
            return ZERO
        average = min(average, remaining_energy_kwh / (minutes / MINUTES_PER_HOUR))
    if phase.is_final:
        # 恢复窗口约束：末段动作必须能在恢复窗口内爬坡回零。
        average = min(average, resource.ramp_kw_per_minute * Decimal(recovery_minutes))
    if tenant_headroom_kw is not None:
        average = min(average, tenant_headroom_kw)
    return max(average, ZERO)


def generate_candidates(
    *,
    phases: Sequence[PhaseInput],
    resources: Sequence[ResourceInput],
    floors: Sequence[TenantFloorInput],
    recovery_minutes: int,
    committed_energy_kwh: Mapping[str, Decimal] | None = None,
) -> list[dict[str, object]]:
    """按既定策略为每个待执行阶段生成候选组合；约束不满足时宁可缺口也不越界。"""
    committed = committed_energy_kwh or {}
    floor_map = {floor.tenant_id: floor for floor in floors}
    candidates: list[dict[str, object]] = []
    for strategy, kind_order in STRATEGIES.items():
        ordered = sorted(
            resources,
            key=lambda item: (kind_order.index(item.kind), -item.adjustable_kw, item.resource_id),
        )
        energy_left = {
            resource.resource_id: (resource.energy_kwh or ZERO) - committed.get(resource.resource_id, ZERO)
            for resource in resources
            if resource.kind == "storage"
        }
        actions: list[dict[str, object]] = []
        phase_rows: list[dict[str, object]] = []
        total_planned = ZERO
        total_shortfall = ZERO
        tenant_impact = ZERO
        for phase in phases:
            tenant_used: dict[str, Decimal] = {}
            remaining = phase.target_reduction_kw
            for resource in ordered:
                if remaining <= ZERO:
                    break
                if resource.protected:
                    continue
                headroom: Decimal | None = None
                if resource.tenant_id is not None:
                    floor = floor_map.get(resource.tenant_id)
                    if floor is None:
                        continue
                    headroom = max(
                        floor.current_load_kw - floor.min_capacity_kw - tenant_used.get(resource.tenant_id, ZERO),
                        ZERO,
                    )
                effective = _effective_kw(
                    resource,
                    phase,
                    energy_left.get(resource.resource_id, ZERO),
                    headroom,
                    recovery_minutes,
                )
                take = floor_kw(min(effective, remaining))
                if take <= ZERO:
                    continue
                actions.append({
                    "phase_seq": phase.phase_seq,
                    "resource_id": resource.resource_id,
                    "kind": resource.kind,
                    "tenant_id": resource.tenant_id,
                    "planned_kw": decimal_text(take),
                    "starts_at": utc_text(phase.starts_at),
                    "ends_at": utc_text(phase.ends_at),
                })
                remaining -= take
                total_planned += take
                if resource.tenant_id is not None:
                    tenant_used[resource.tenant_id] = tenant_used.get(resource.tenant_id, ZERO) + take
                    tenant_impact += take
                if resource.kind == "storage":
                    energy_left[resource.resource_id] -= take * (phase.minutes / MINUTES_PER_HOUR)
            shortfall = floor_kw(remaining)
            total_shortfall += shortfall
            phase_rows.append({
                "phase_seq": phase.phase_seq,
                "target_reduction_kw": decimal_text(phase.target_reduction_kw),
                "planned_kw": decimal_text(phase.target_reduction_kw - shortfall),
                "shortfall_kw": decimal_text(shortfall),
            })
        candidates.append({
            "strategy": strategy,
            "feasible": total_shortfall == ZERO,
            "actions": actions,
            "phases": phase_rows,
            "total_planned_kw": total_planned,
            "total_shortfall_kw": total_shortfall,
            "tenant_impact_kw": tenant_impact,
        })
    candidates.sort(key=lambda item: (item["total_shortfall_kw"], item["tenant_impact_kw"], item["strategy"]))
    for candidate in candidates:
        candidate["total_planned_kw"] = decimal_text(candidate["total_planned_kw"])
        candidate["total_shortfall_kw"] = decimal_text(candidate["total_shortfall_kw"])
        candidate["tenant_impact_kw"] = decimal_text(candidate["tenant_impact_kw"])
    return candidates


def shortfall_reasons(
    *,
    target_kw: Decimal,
    planned_kw: Decimal,
    actual_kw: Decimal,
    receipts_applied: bool,
    any_failed: bool,
    phase_started: bool,
    phase_ended: bool,
) -> list[str]:
    """按确定性顺序解释阶段未达标原因。"""
    reasons: list[str] = []
    if planned_kw < target_kw:
        reasons.append("insufficient_resources")
    if any_failed:
        reasons.append("action_failed")
    if receipts_applied and actual_kw < planned_kw:
        reasons.append("partial_delivery")
    if phase_ended and not receipts_applied:
        reasons.append("receipts_missing")
    if not reasons and not phase_started and planned_kw >= target_kw:
        reasons.append("awaiting_execution")
    return reasons


def recovery_minutes_needed(planned_kw: Decimal, ramp_kw_per_minute: Decimal) -> int:
    if planned_kw <= ZERO:
        return 0
    return int((planned_kw / ramp_kw_per_minute).to_integral_value(rounding=ROUND_CEILING))


def achievement_percent(actual_kw: Decimal, target_kw: Decimal) -> Decimal | None:
    if target_kw <= ZERO:
        return None
    return (actual_kw / target_kw * HUNDRED).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
