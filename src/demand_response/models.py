"""需求响应领域输入契约：指令版本、削减曲线、站点基线与资源可调属性。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .clock import parse_utc
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
RESOURCE_KINDS = {"job", "cap", "storage"}
DIRECTIVE_STATES = {"draft", "confirmed", "executing", "closed", "cancelled"}


def required_text(value: object, field_name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field_name} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field_name} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field_name: str) -> str:
    result = required_text(value, field_name, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field_name} 格式不正确")
    return result


def decimal_value(
    value: object,
    field_name: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field_name} 必须是数值")
    try:
        result = Decimal(str(value))
    except Exception as exc:  # InvalidOperation/ValueError
        raise ValidationFailed(f"{field_name} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field_name} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field_name} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field_name} 不能大于 {maximum}")
    return result


def boolean_value(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationFailed(f"{field_name} 必须是布尔值")
    return value


def time_value(value: object, field_name: str) -> str:
    text = required_text(value, field_name, 40)
    try:
        parse_utc(text, field_name)
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc
    return text


@dataclass(frozen=True, slots=True)
class CurvePoint:
    stage_id: str
    starts_at: str
    ends_at: str
    target_reduction_kw: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CurvePoint":
        start = time_value(raw.get("starts_at"), "starts_at")
        end = time_value(raw.get("ends_at"), "ends_at")
        if parse_utc(end) <= parse_utc(start):
            raise ValidationFailed("削减阶段 ends_at 必须晚于 starts_at")
        return cls(
            stage_id=identifier(raw.get("stage_id"), "stage_id"),
            starts_at=start,
            ends_at=end,
            target_reduction_kw=decimal_value(
                raw.get("target_reduction_kw"), "target_reduction_kw", minimum=Decimal("0")
            ),
        )


@dataclass(frozen=True, slots=True)
class DirectiveRequest:
    """电网需求响应指令的一个版本。"""

    directive_id: str
    source_revision: str
    window_start: str
    window_end: str
    recover_by_at: str
    curve: tuple[CurvePoint, ...]
    baselines: tuple[tuple[str, str, Decimal], ...]
    note: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DirectiveRequest":
        directive_id = identifier(raw.get("directive_id"), "directive_id")
        source_revision = identifier(raw.get("source_revision"), "source_revision")
        window_start = time_value(raw.get("window_start"), "window_start")
        window_end = time_value(raw.get("window_end"), "window_end")
        recover_by_at = time_value(raw.get("recover_by_at"), "recover_by_at")
        if parse_utc(window_end) <= parse_utc(window_start):
            raise ValidationFailed("承诺窗口 window_end 必须晚于 window_start")
        if parse_utc(recover_by_at) < parse_utc(window_end):
            raise ValidationFailed("恢复窗口 recover_by_at 不能早于承诺窗口结束时间")
        raw_curve = raw.get("curve")
        if not isinstance(raw_curve, list) or not raw_curve:
            raise ValidationFailed("curve 必须是非空阶段数组")
        points = tuple(CurvePoint.from_dict(item) for item in raw_curve)
        stage_ids = [point.stage_id for point in points]
        if len(set(stage_ids)) != len(stage_ids):
            raise ValidationFailed("削减阶段 stage_id 不能重复")
        ordered = sorted(points, key=lambda item: parse_utc(item.starts_at))
        if [item.stage_id for item in ordered] != stage_ids:
            raise ValidationFailed("削减曲线必须按 starts_at 升序提供")
        for point in points:
            if parse_utc(point.starts_at) < parse_utc(window_start) or parse_utc(point.ends_at) > parse_utc(window_end):
                raise ValidationFailed(f"阶段 {point.stage_id} 超出承诺窗口")
        for previous, current in zip(ordered, ordered[1:]):
            if parse_utc(current.starts_at) != parse_utc(previous.ends_at):
                raise ValidationFailed("削减阶段必须首尾相接、不能重叠或留空")
        raw_baselines = raw.get("baselines", [])
        if not isinstance(raw_baselines, list) or not raw_baselines:
            raise ValidationFailed("baselines 必须是非空数组")
        baselines: list[tuple[str, str, Decimal]] = []
        seen: set[tuple[str, str]] = set()
        for item in raw_baselines:
            if not isinstance(item, Mapping):
                raise ValidationFailed("baselines 条目必须是对象")
            site_id = identifier(item.get("site_id"), "site_id")
            stage_id = identifier(item.get("stage_id"), "baselines.stage_id")
            if stage_id not in stage_ids:
                raise ValidationFailed(f"基线阶段 {stage_id} 未在削减曲线中声明")
            key = (site_id, stage_id)
            if key in seen:
                raise ValidationFailed(f"站点 {site_id} 阶段 {stage_id} 基线重复")
            seen.add(key)
            baseline = decimal_value(item.get("baseline_kw"), "baseline_kw", minimum=Decimal("0"))
            baselines.append((site_id, stage_id, baseline))
        note = required_text(raw.get("note", "电网需求响应指令"), "note")
        return cls(
            directive_id=directive_id,
            source_revision=source_revision,
            window_start=window_start,
            window_end=window_end,
            recover_by_at=recover_by_at,
            curve=points,
            baselines=tuple(baselines),
            note=note,
        )

    def stage_index(self) -> dict[str, CurvePoint]:
        return {point.stage_id: point for point in self.curve}

    def sites(self) -> list[str]:
        return sorted({site_id for site_id, _, _ in self.baselines})


@dataclass(frozen=True, slots=True)
class ResourceRequest:
    """机房侧可调资源：延迟作业、功率封顶或储能放电。

    各 kind 的 detail 字段：
    - job：tenant_id、load_kw、stages、interruptible、resume_by_at、duration_minutes
    - cap：tenant_id、current_kw、max_reduction_kw、ramp_kw_per_stage
    - storage：available_kwh、discharge_kw、ramp_kw_per_stage、recharge_kw
    """

    resource_id: str
    site_id: str
    kind: str
    tenant_id: str | None
    detail: Mapping[str, Any]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResourceRequest":
        resource_id = identifier(raw.get("resource_id"), "resource_id")
        site_id = identifier(raw.get("site_id"), "site_id")
        kind = required_text(raw.get("kind"), "kind", 16)
        if kind not in RESOURCE_KINDS:
            raise ValidationFailed("kind 必须是 job、cap 或 storage")
        detail = raw.get("detail")
        if not isinstance(detail, Mapping):
            raise ValidationFailed("detail 必须是对象")
        tenant_raw = detail.get("tenant_id")
        tenant_id = None if tenant_raw is None else identifier(tenant_raw, "detail.tenant_id")
        if kind in {"job", "cap"} and tenant_id is None:
            raise ValidationFailed(f"{kind} 资源必须声明 tenant_id")
        if kind == "job":
            cls._parse_job(detail)
        elif kind == "cap":
            cls._parse_cap(detail)
        else:
            cls._parse_storage(detail)
        return cls(resource_id=resource_id, site_id=site_id, kind=kind, tenant_id=tenant_id, detail=dict(detail))

    @staticmethod
    def _parse_job(detail: Mapping[str, Any]) -> None:
        stages = detail.get("stages")
        if not isinstance(stages, list) or not stages:
            raise ValidationFailed("job 资源必须声明可延迟阶段 stages")
        for stage_id in stages:
            identifier(stage_id, "detail.stages")
        if len(set(stages)) != len(stages):
            raise ValidationFailed("job 资源 stages 不能重复")
        boolean_value(detail.get("interruptible"), "detail.interruptible")
        decimal_value(detail.get("load_kw"), "detail.load_kw", minimum=Decimal("0.001"))
        duration = decimal_value(
            detail.get("duration_minutes"), "detail.duration_minutes", minimum=Decimal("0.001")
        )
        resume_by = time_value(detail.get("resume_by_at"), "detail.resume_by_at")
        # 恢复窗口必须足以容纳整个作业时长，延迟才有意义。
        if duration <= 0:
            raise ValidationFailed("detail.duration_minutes 必须为正")

    @staticmethod
    def _parse_cap(detail: Mapping[str, Any]) -> None:
        current = decimal_value(detail.get("current_kw"), "detail.current_kw", minimum=Decimal("0"))
        max_reduction = decimal_value(
            detail.get("max_reduction_kw"), "detail.max_reduction_kw", minimum=Decimal("0")
        )
        floor = decimal_value(detail.get("floor_kw", 0), "detail.floor_kw", minimum=Decimal("0"))
        if floor > current:
            raise ValidationFailed("detail.floor_kw 不能超过 current_kw")
        if max_reduction > current - floor:
            raise ValidationFailed("detail.max_reduction_kw 超过 current_kw 与租户保底 floor_kw 的差额")
        decimal_value(
            detail.get("ramp_kw_per_stage"), "detail.ramp_kw_per_stage", minimum=Decimal("0.001")
        )

    @staticmethod
    def _parse_storage(detail: Mapping[str, Any]) -> None:
        decimal_value(detail.get("available_kwh"), "detail.available_kwh", minimum=Decimal("0"))
        decimal_value(detail.get("discharge_kw"), "detail.discharge_kw", minimum=Decimal("0.001"))
        decimal_value(
            detail.get("ramp_kw_per_stage"), "detail.ramp_kw_per_stage", minimum=Decimal("0.001")
        )
        decimal_value(detail.get("recharge_kw"), "detail.recharge_kw", minimum=Decimal("0.001"))


@dataclass(frozen=True, slots=True)
class TenantGuarantee:
    site_id: str
    tenant_id: str
    min_compute_kw: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TenantGuarantee":
        return cls(
            site_id=identifier(raw.get("site_id"), "site_id"),
            tenant_id=identifier(raw.get("tenant_id"), "tenant_id"),
            min_compute_kw=decimal_value(
                raw.get("min_compute_kw"), "min_compute_kw", minimum=Decimal("0")
            ),
        )


# 回执事件合法类型；状态机迁移规则见 service.ALLOWED_TRANSITIONS。
RECEIPT_EVENT_TYPES = {"dispatched", "achieved", "failed", "restored"}


@dataclass(frozen=True, slots=True)
class ReceiptEvent:
    directive_id: str
    source_event_id: str
    stage_id: str
    resource_id: str
    event_type: str
    event_time: str
    observed_reduction_kw: Decimal | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ReceiptEvent":
        event_type = required_text(raw.get("event_type"), "event_type", 24)
        if event_type not in RECEIPT_EVENT_TYPES:
            raise ValidationFailed("event_type 必须是 dispatched、achieved、failed 或 restored")
        observed = raw.get("observed_reduction_kw")
        if observed is not None:
            observed = decimal_value(observed, "observed_reduction_kw", minimum=Decimal("0"))
        elif event_type == "achieved":
            raise ValidationFailed("achieved 回执必须携带 observed_reduction_kw")
        return cls(
            directive_id=identifier(raw.get("directive_id"), "directive_id"),
            source_event_id=identifier(raw.get("source_event_id"), "source_event_id"),
            stage_id=identifier(raw.get("stage_id"), "stage_id"),
            resource_id=identifier(raw.get("resource_id"), "resource_id"),
            event_type=event_type,
            event_time=time_value(raw.get("event_time"), "event_time"),
            observed_reduction_kw=observed,
        )
