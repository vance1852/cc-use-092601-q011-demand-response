"""需求响应协同模块的领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from .clock import parse_utc
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
RESOURCE_KINDS = {"job_delay", "power_cap", "storage"}
RECEIPT_STATUSES = {"started", "achieved", "partial", "failed", "recovery_started", "recovered"}
TENANT_KINDS = {"job_delay", "power_cap"}


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def integer_value(value: object, field: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationFailed(f"{field} 必须是 {minimum} 到 {maximum} 的整数")
    return value


def utc_field(value: object, field: str) -> str:
    result = required_text(value, field, 40)
    try:
        parse_utc(result, field)
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc
    return result


@dataclass(frozen=True, slots=True)
class CurveInterval:
    starts_at: str
    ends_at: str
    target_reduction_kw: Decimal


def parse_curve(raw: object, field: str = "curve") -> tuple[CurveInterval, ...]:
    if isinstance(raw, str) or not isinstance(raw, Sequence) or not raw:
        raise ValidationFailed(f"{field} 必须是非空数组")
    intervals: list[CurveInterval] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValidationFailed(f"{field}[{index}] 必须是对象")
        starts_at = utc_field(item.get("starts_at"), f"{field}[{index}].starts_at")
        ends_at = utc_field(item.get("ends_at"), f"{field}[{index}].ends_at")
        if parse_utc(ends_at) <= parse_utc(starts_at):
            raise ValidationFailed(f"{field}[{index}] 结束必须晚于开始")
        intervals.append(
            CurveInterval(
                starts_at=starts_at,
                ends_at=ends_at,
                target_reduction_kw=decimal_value(
                    item.get("target_reduction_kw"),
                    f"{field}[{index}].target_reduction_kw",
                    minimum=Decimal("0.001"),
                ),
            )
        )
    ordered = sorted(intervals, key=lambda interval: (interval.starts_at, interval.ends_at))
    for left, right in zip(ordered, ordered[1:]):
        if right.starts_at < left.ends_at:
            raise ValidationFailed(f"{field} 区间不能重叠")
    return tuple(ordered)


@dataclass(frozen=True, slots=True)
class SiteInput:
    site_id: str
    name: str
    timezone: str
    baseline_kw: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "SiteInput":
        timezone = required_text(raw.get("timezone"), "timezone", 64)
        if "/" not in timezone and timezone != "UTC":
            raise ValidationFailed("timezone 必须是 IANA 时区或 UTC")
        return cls(
            site_id=identifier(raw.get("site_id"), "site_id"),
            name=required_text(raw.get("name"), "name"),
            timezone=timezone,
            baseline_kw=decimal_value(raw.get("baseline_kw"), "baseline_kw", minimum=Decimal("0.001")),
        )


@dataclass(frozen=True, slots=True)
class TenantFloorInput:
    tenant_id: str
    current_load_kw: Decimal
    min_capacity_kw: Decimal

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TenantFloorInput":
        current = decimal_value(raw.get("current_load_kw"), "current_load_kw", minimum=Decimal("0"))
        minimum = decimal_value(raw.get("min_capacity_kw"), "min_capacity_kw", minimum=Decimal("0"))
        if minimum > current:
            raise ValidationFailed("min_capacity_kw 不能高于 current_load_kw")
        return cls(
            tenant_id=identifier(raw.get("tenant_id"), "tenant_id"),
            current_load_kw=current,
            min_capacity_kw=minimum,
        )


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

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ResourceInput":
        kind = required_text(raw.get("kind"), "kind", 32)
        if kind not in RESOURCE_KINDS:
            raise ValidationFailed("kind 必须是 job_delay、power_cap 或 storage")
        tenant_raw = raw.get("tenant_id")
        tenant_id = None if tenant_raw is None else identifier(tenant_raw, "tenant_id")
        if kind in TENANT_KINDS and tenant_id is None:
            raise ValidationFailed(f"{kind} 资源必须登记 tenant_id")
        if kind == "storage" and tenant_id is not None:
            raise ValidationFailed("storage 资源不属于租户")
        delay_raw = raw.get("max_delay_minutes")
        energy_raw = raw.get("energy_kwh")
        max_delay = None
        energy = None
        if kind == "job_delay":
            max_delay = integer_value(delay_raw, "max_delay_minutes", minimum=1, maximum=1440)
        elif delay_raw is not None:
            raise ValidationFailed("只有 job_delay 资源可以登记 max_delay_minutes")
        if kind == "storage":
            energy = decimal_value(energy_raw, "energy_kwh", minimum=Decimal("0.001"))
        elif energy_raw is not None:
            raise ValidationFailed("只有 storage 资源可以登记 energy_kwh")
        protected = raw.get("protected", False)
        if not isinstance(protected, bool):
            raise ValidationFailed("protected 必须是布尔值")
        return cls(
            resource_id=identifier(raw.get("resource_id"), "resource_id"),
            kind=kind,
            tenant_id=tenant_id,
            adjustable_kw=decimal_value(
                raw.get("adjustable_kw"), "adjustable_kw", minimum=Decimal("0.001")
            ),
            ramp_kw_per_minute=decimal_value(
                raw.get("ramp_kw_per_minute"), "ramp_kw_per_minute", minimum=Decimal("0.001")
            ),
            max_delay_minutes=max_delay,
            energy_kwh=energy,
            protected=protected,
        )


@dataclass(frozen=True, slots=True)
class EventInput:
    event_id: str
    site_id: str
    window_start: str
    window_end: str
    recovery_minutes: int
    curve: tuple[CurveInterval, ...]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "EventInput":
        return cls(
            event_id=identifier(raw.get("event_id"), "event_id"),
            site_id=identifier(raw.get("site_id"), "site_id"),
            window_start=utc_field(raw.get("window_start"), "window_start"),
            window_end=utc_field(raw.get("window_end"), "window_end"),
            recovery_minutes=integer_value(
                raw.get("recovery_minutes"), "recovery_minutes", minimum=0, maximum=1440
            ),
            curve=parse_curve(raw.get("curve")),
        )


@dataclass(frozen=True, slots=True)
class ReceiptInput:
    receipt_id: str
    action_id: str
    status: str
    measured_kw: Decimal
    occurred_at: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ReceiptInput":
        status = required_text(raw.get("status"), "status", 32)
        if status not in RECEIPT_STATUSES:
            raise ValidationFailed("status 不是受支持的回执状态")
        return cls(
            receipt_id=identifier(raw.get("receipt_id"), "receipt_id"),
            action_id=identifier(raw.get("action_id"), "action_id"),
            status=status,
            measured_kw=decimal_value(raw.get("measured_kw"), "measured_kw", minimum=Decimal("0")),
            occurred_at=utc_field(raw.get("occurred_at"), "occurred_at"),
        )
