"""需求响应协同的事务用例：指令版本、候选组合、资源冻结与回执归并。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Callable, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import DirectiveRequest, ReceiptEvent, ResourceRequest, TenantGuarantee
from .planning import build_plan, canonical_json, digest, q, text
from .storage import initialize, transaction


ZERO = Decimal("0")
# 执行状态机：achieved/failed 互为终态、互不可覆盖，只允许向 restored 收敛；
# restored 为最终终态，任何后续事件都被丢弃。
ALLOWED_TRANSITIONS = {
    "locked": {"dispatched", "achieved", "failed"},
    "dispatched": {"achieved", "failed", "restored"},
    "achieved": {"restored"},
    "failed": {"restored"},
    "restored": set(),
}
ROLE_PERMISSIONS = {
    "operator": {"directive.write", "resource.write", "candidate.generate", "receipt.write", "report.read"},
    "approver": {"candidate.confirm", "directive.close", "report.read"},
    "auditor": {"report.read", "audit.read"},
}


class DemandResponseService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    # ---------- 基础辅助 ----------

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dr_users WHERE user_id=?", (user_id,)
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

    def _audit(self, entity_type: str, entity_id: str, event_type: str, actor_id: str,
               payload: Mapping[str, Any]) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM dr_audit_events ORDER BY event_id DESC LIMIT 1"
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
            "INSERT INTO dr_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload),
             previous_hash, event_hash, body["created_at"]),
        )

    def _idempotent(self, scope: str, key: str, request: Mapping[str, Any],
                    producer: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        request_sha = digest(request)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM dr_idempotency WHERE scope=? AND idempotency_key=?",
            (scope, key),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_sha:
                raise Conflict("幂等键对应不同的请求内容")
            return json.loads(stored["response_json"])
        response = producer()
        self.connection.execute(
            "INSERT INTO dr_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
            "VALUES(?,?,?,?,?)",
            (scope, key, request_sha, canonical_json(response), self._now()),
        )
        return response

    def _head(self, directive_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dr_directive_head WHERE directive_id=?", (directive_id,)
        ).fetchone()
        if row is None:
            raise NotFound("需求响应指令不存在")
        return row

    def _version_row(self, directive_id: str, version: int) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dr_directives WHERE directive_id=? AND version=?",
            (directive_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("指令版本不存在")
        return row

    def _locked_stages(self, directive_id: str) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT stage_id,locked_version FROM dr_stage_locks WHERE directive_id=?",
            (directive_id,),
        ).fetchall()
        return {row["stage_id"]: row["locked_version"] for row in rows}

    def _guarantees(self, directive_id: str, version: int) -> dict[tuple[str, str], Decimal]:
        rows = self.connection.execute(
            "SELECT site_id,tenant_id,min_compute_kw FROM dr_tenant_guarantees "
            "WHERE directive_id=? AND version=?",
            (directive_id, version),
        ).fetchall()
        return {(row["site_id"], row["tenant_id"]): Decimal(row["min_compute_kw"]) for row in rows}

    def _baselines(self, directive_id: str, version: int) -> dict[tuple[str, str], Decimal]:
        rows = self.connection.execute(
            "SELECT site_id,stage_id,baseline_kw FROM dr_baselines WHERE directive_id=? AND version=?",
            (directive_id, version),
        ).fetchall()
        return {(row["site_id"], row["stage_id"]): Decimal(row["baseline_kw"]) for row in rows}

    def _resources(self, directive_id: str, version: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM dr_resources WHERE directive_id=? AND version=? AND active=1 "
            "ORDER BY resource_id",
            (directive_id, version),
        ).fetchall()
        return [
            {
                "resource_id": row["resource_id"],
                "site_id": row["site_id"],
                "kind": row["kind"],
                "tenant_id": row["tenant_id"],
                "detail": json.loads(row["detail_json"]),
            }
            for row in rows
        ]

    def _insert_version(self, actor_id: str, req: DirectiveRequest, version: int,
                        supersedes: int | None, guarantees: list[TenantGuarantee]) -> None:
        definition = {
            "directive_id": req.directive_id,
            "source_revision": req.source_revision,
            "window_start": req.window_start,
            "window_end": req.window_end,
            "recover_by_at": req.recover_by_at,
            "curve": [
                {
                    "stage_id": point.stage_id,
                    "starts_at": point.starts_at,
                    "ends_at": point.ends_at,
                    "target_reduction_kw": text(point.target_reduction_kw),
                }
                for point in req.curve
            ],
            "baselines": [
                {"site_id": site_id, "stage_id": stage_id, "baseline_kw": text(baseline)}
                for site_id, stage_id, baseline in req.baselines
            ],
            "guarantees": [
                {"site_id": g.site_id, "tenant_id": g.tenant_id, "min_compute_kw": text(g.min_compute_kw)}
                for g in guarantees
            ],
            "note": req.note,
        }
        content_sha256 = digest(definition)
        self.connection.execute(
            "INSERT INTO dr_directives(directive_id,version,source_revision,window_start,window_end,"
            "recover_by_at,state,definition_json,content_sha256,supersedes_version,created_by,created_at) "
            "VALUES(?,?,?,?,?,?,'draft',?,?,?,?,?)",
            (req.directive_id, version, req.source_revision, req.window_start, req.window_end,
             req.recover_by_at, canonical_json(definition), content_sha256, supersedes,
             actor_id, self._now()),
        )
        for site_id, stage_id, baseline in req.baselines:
            self.connection.execute(
                "INSERT INTO dr_baselines(directive_id,version,site_id,stage_id,baseline_kw) "
                "VALUES(?,?,?,?,?)",
                (req.directive_id, version, site_id, stage_id, text(baseline)),
            )
        for guarantee in guarantees:
            self.connection.execute(
                "INSERT INTO dr_tenant_guarantees(directive_id,version,site_id,tenant_id,min_compute_kw) "
                "VALUES(?,?,?,?,?)",
                (req.directive_id, version, guarantee.site_id, guarantee.tenant_id,
                 text(guarantee.min_compute_kw)),
            )

    @staticmethod
    def _parse_guarantees(raw: Mapping[str, Any], sites: set[str]) -> list[TenantGuarantee]:
        raw_list = raw.get("guarantees", [])
        if not isinstance(raw_list, list):
            raise ValidationFailed("guarantees 必须是数组")
        guarantees = [TenantGuarantee.from_dict(item) for item in raw_list]
        seen: set[tuple[str, str]] = set()
        for guarantee in guarantees:
            if guarantee.site_id not in sites:
                raise ValidationFailed(f"保底站点 {guarantee.site_id} 未在基线中声明")
            key = (guarantee.site_id, guarantee.tenant_id)
            if key in seen:
                raise ValidationFailed(f"租户 {guarantee.tenant_id} 在站点 {guarantee.site_id} 的保底重复")
            seen.add(key)
        return guarantees

    # ---------- 用户 ----------

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dr_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    # ---------- 指令登记与修订 ----------

    def register_directive(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "directive.write")
        idempotency_key = raw.get("idempotency_key")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationFailed("idempotency_key 不能为空")

        def produce() -> dict[str, Any]:
            req = DirectiveRequest.from_dict(raw)
            guarantees = self._parse_guarantees(raw, set(req.sites()))
            exists = self.connection.execute(
                "SELECT 1 FROM dr_directive_head WHERE directive_id=?", (req.directive_id,)
            ).fetchone()
            if exists is not None:
                raise Conflict("指令编号已经存在，请使用修订接口")
            with transaction(self.connection, immediate=True):
                self._insert_version(actor_id, req, 1, None, guarantees)
                self.connection.execute(
                    "INSERT INTO dr_directive_head(directive_id,current_version,state,updated_at) "
                    "VALUES(?,1,'draft',?)",
                    (req.directive_id, self._now()),
                )
                self._audit("directive", req.directive_id, "directive.registered", actor_id,
                            {"version": 1, "source_revision": req.source_revision})
            return {"directive_id": req.directive_id, "version": 1, "state": "draft"}

        return self._idempotent("directive.register", idempotency_key.strip(), dict(raw), produce)

    def revise_directive(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """登记指令修订版本：只允许调整未锁定阶段，已锁定阶段曲线必须保持一致。"""
        self._require(actor_id, "directive.write")
        idempotency_key = raw.get("idempotency_key")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationFailed("idempotency_key 不能为空")

        def produce() -> dict[str, Any]:
            req = DirectiveRequest.from_dict(raw)
            guarantees = self._parse_guarantees(raw, set(req.sites()))
            head = self._head(req.directive_id)
            if head["state"] in {"closed", "cancelled"}:
                raise InvalidState("指令已关闭或取消，不能修订")
            current = self._version_row(req.directive_id, head["current_version"])
            locked = self._locked_stages(req.directive_id)
            old_definition = json.loads(current["definition_json"])
            old_stage_ids = {item["stage_id"] for item in old_definition["curve"]}
            if set(locked) == old_stage_ids:
                raise InvalidState("所有阶段均已开始执行，没有可修订的未执行阶段")
            if locked:
                old_req = DirectiveRequest.from_dict({**old_definition, "guarantees": []})
                old_stages = old_req.stage_index()
                new_stages = req.stage_index()
                for stage_id in locked:
                    if stage_id not in new_stages:
                        raise Conflict(f"阶段 {stage_id} 已开始执行，修订不能移除")
                    if new_stages[stage_id] != old_stages[stage_id]:
                        raise Conflict(f"阶段 {stage_id} 已开始执行，曲线不可变更")
            new_version = int(head["current_version"]) + 1
            with transaction(self.connection, immediate=True):
                self._insert_version(actor_id, req, new_version, head["current_version"], guarantees)
                # 资源快照随版本复制，运营可在新版本上继续调整。
                self.connection.execute(
                    "INSERT INTO dr_resources(resource_id,directive_id,version,site_id,kind,tenant_id,"
                    "detail_json,content_sha256,active,created_by,created_at) "
                    "SELECT resource_id,directive_id,?,site_id,kind,tenant_id,detail_json,content_sha256,"
                    "active,?,? FROM dr_resources WHERE directive_id=? AND version=?",
                    (new_version, actor_id, self._now(), req.directive_id, head["current_version"]),
                )
                self.connection.execute(
                    "UPDATE dr_directive_head SET current_version=?,updated_at=? WHERE directive_id=?",
                    (new_version, self._now(), req.directive_id),
                )
                self._audit("directive", req.directive_id, "directive.revised", actor_id, {
                    "version": new_version,
                    "source_revision": req.source_revision,
                    "locked_stages": sorted(locked),
                })
            return {"directive_id": req.directive_id, "version": new_version, "state": "draft",
                    "locked_stages": sorted(locked)}

        return self._idempotent("directive.revise", idempotency_key.strip(), dict(raw), produce)

    # ---------- 资源可调属性 ----------

    def register_resource(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "resource.write")
        resource = ResourceRequest.from_dict(raw)
        directive_id = raw.get("directive_id")
        if not isinstance(directive_id, str) or not directive_id.strip():
            raise ValidationFailed("directive_id 不能为空")
        directive_id = directive_id.strip()
        head = self._head(directive_id)
        version_row = self._version_row(directive_id, head["current_version"])
        if version_row["state"] != "draft":
            raise InvalidState("当前指令版本已确认，请先修订再调整资源")
        definition = json.loads(version_row["definition_json"])
        sites = {item["site_id"] for item in definition["baselines"]}
        if resource.site_id not in sites:
            raise ValidationFailed(f"站点 {resource.site_id} 未在指令基线中声明")
        stage_ids = {item["stage_id"] for item in definition["curve"]}
        if resource.kind == "job":
            unknown = set(resource.detail["stages"]) - stage_ids
            if unknown:
                raise ValidationFailed(f"作业声明的阶段 {sorted(unknown)} 不在削减曲线中")
            if parse_utc(resource.detail["resume_by_at"]) < parse_utc(definition["window_end"]):
                # 仍允许登记，但规划时会被排除；这里仅提示性校验通过。
                pass
        detail_json = canonical_json(resource.detail)
        content_sha256 = digest({"resource_id": resource.resource_id, "detail": resource.detail,
                                 "kind": resource.kind, "site_id": resource.site_id})
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "INSERT INTO dr_resources(resource_id,directive_id,version,site_id,kind,tenant_id,"
                "detail_json,content_sha256,active,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,1,?,?) "
                "ON CONFLICT(resource_id,directive_id,version) DO UPDATE SET "
                "site_id=excluded.site_id,kind=excluded.kind,tenant_id=excluded.tenant_id,"
                "detail_json=excluded.detail_json,content_sha256=excluded.content_sha256,active=1",
                (resource.resource_id, directive_id, head["current_version"], resource.site_id,
                 resource.kind, resource.tenant_id, detail_json, content_sha256,
                 actor_id, self._now()),
            )
            self._audit("resource", resource.resource_id, "resource.registered", actor_id, {
                "directive_id": directive_id,
                "version": head["current_version"],
                "kind": resource.kind,
            })
        return {"resource_id": resource.resource_id, "directive_id": directive_id,
                "version": head["current_version"], "kind": resource.kind, "active": True}

    def retire_resource(self, actor_id: str, directive_id: str, resource_id: str) -> dict[str, Any]:
        self._require(actor_id, "resource.write")
        head = self._head(directive_id)
        version_row = self._version_row(directive_id, head["current_version"])
        if version_row["state"] != "draft":
            raise InvalidState("当前指令版本已确认，请先修订再调整资源")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE dr_resources SET active=0 WHERE resource_id=? AND directive_id=? AND version=? "
                "AND active=1",
                (resource_id, directive_id, head["current_version"]),
            )
            if cursor.rowcount != 1:
                raise NotFound("资源不存在或已停用")
            self._audit("resource", resource_id, "resource.retired", actor_id,
                        {"directive_id": directive_id, "version": head["current_version"]})
        return {"resource_id": resource_id, "active": False}

    # ---------- 候选组合 ----------

    def _plan_inputs(self, directive_id: str, version: int):
        version_row = self._version_row(directive_id, version)
        definition = json.loads(version_row["definition_json"])
        locked = self._locked_stages(directive_id)
        remaining = [item for item in definition["curve"] if item["stage_id"] not in locked]
        baselines = {
            key: value
            for key, value in self._baselines(directive_id, version).items()
            if key[1] in {item["stage_id"] for item in remaining}
        }
        guarantees = self._guarantees(directive_id, version)
        resources = self._resources(directive_id, version)
        remaining_ids = {item["stage_id"] for item in remaining}
        planned_resources: list[dict[str, Any]] = []
        for resource in resources:
            if resource["kind"] == "job":
                stages = [s for s in resource["detail"]["stages"] if s in remaining_ids]
                if not stages:
                    continue
                resource = {**resource, "detail": {**resource["detail"], "stages": stages}}
            planned_resources.append(resource)
        return version_row, definition, remaining, baselines, guarantees, planned_resources

    def _locked_prior_state(self, directive_id: str, resources: list[dict[str, Any]]):
        """汇总已锁定阶段末尾的执行状态，供修订后重排未锁阶段时沿续。"""
        locked = self._locked_stages(directive_id)
        rows = self.connection.execute(
            "SELECT * FROM dr_frozen_actions WHERE directive_id=? AND state<>'superseded'",
            (directive_id,),
        ).fetchall()
        jobs_initial: set[str] = set()
        caps_initial: dict[str, Decimal] = {}
        storage_energy: dict[str, Decimal] = {}
        storage_power: dict[str, tuple[str, Decimal]] = {}
        hours_by_stage: dict[str, Decimal] = {}
        head = self._head(directive_id)
        version_row = self._version_row(directive_id, head["current_version"])
        ordered_stages = json.loads(version_row["definition_json"])["curve"]
        stage_order = {item["stage_id"]: index for index, item in enumerate(ordered_stages)}
        for item in ordered_stages:
            hours_by_stage[item["stage_id"]] = Decimal(
                (parse_utc(item["ends_at"]) - parse_utc(item["starts_at"])).total_seconds()
            ) / Decimal(3600)
        for row in rows:
            if row["stage_id"] not in locked:
                # 只沿续已锁定阶段；未锁阶段的旧版本动作等待本次确认时作废。
                continue
            power = Decimal(row["reduction_kw"])
            if row["kind"] == "job":
                jobs_initial.add(row["resource_id"])
            elif row["kind"] == "cap":
                caps_initial[row["resource_id"]] = power
            else:
                previous = storage_power.get(row["resource_id"])
                if previous is None or stage_order.get(row["stage_id"], -1) > stage_order.get(previous[0], -1):
                    storage_power[row["resource_id"]] = (row["stage_id"], power)
                storage_energy[row["resource_id"]] = storage_energy.get(row["resource_id"], ZERO) + (
                    power * hours_by_stage.get(row["stage_id"], ZERO)
                )
        storage_initial = {
            resource_id: {
                "reduction_kw": storage_power.get(resource_id, ("", ZERO))[1],
                "discharged_kwh": q(storage_energy.get(resource_id, ZERO)),
            }
            for resource_id in set(storage_power) | set(storage_energy)
        }
        return jobs_initial, caps_initial, storage_initial

    def generate_candidates(self, actor_id: str, directive_id: str,
                            version: int | None = None) -> dict[str, Any]:
        self._require(actor_id, "candidate.generate")
        head = self._head(directive_id)
        version = head["current_version"] if version is None else int(version)
        version_row, definition, remaining, baselines, guarantees, resources = self._plan_inputs(
            directive_id, version
        )
        if version_row["state"] != "draft":
            raise InvalidState("该指令版本已确认，不能再生成候选组合")
        if head["state"] in {"closed", "cancelled"}:
            raise InvalidState("指令已关闭或取消")
        if not remaining:
            raise InvalidState("所有阶段均已锁定执行，无需生成候选组合")
        if not resources:
            raise InvalidState("没有可调资源，无法生成候选组合")
        jobs_initial, caps_initial, storage_initial = self._locked_prior_state(
            directive_id, resources
        )
        input_value = {
            "definition_sha256": version_row["content_sha256"],
            "stages": [item["stage_id"] for item in remaining],
            "resources": [
                {"resource_id": r["resource_id"], "detail": r["detail"], "kind": r["kind"],
                 "site_id": r["site_id"]}
                for r in resources
            ],
            "guarantees": {f"{k[0]}|{k[1]}": text(v) for k, v in sorted(guarantees.items())},
        }
        input_sha256 = digest(input_value)
        existing = self.connection.execute(
            "SELECT candidate_id,plan_json,state FROM dr_candidates "
            "WHERE directive_id=? AND version=? AND input_sha256=?",
            (directive_id, version, input_sha256),
        ).fetchone()
        if existing is not None:
            plan = json.loads(existing["plan_json"])
            return {"candidate_id": existing["candidate_id"], "directive_id": directive_id,
                    "version": version, "feasible": plan["feasible"], "replayed": True,
                    "state": existing["state"], "plan": plan}
        plan = build_plan(
            directive_id=directive_id,
            source_revision=definition["source_revision"],
            window_end=definition["window_end"],
            recover_by_at=definition["recover_by_at"],
            stages_raw=remaining,
            baselines=baselines,
            guarantees=guarantees,
            raw_resources=resources,
            jobs_initial=jobs_initial,
            caps_initial=caps_initial,
            storage_initial=storage_initial,
        )
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO dr_candidates(directive_id,version,plan_json,input_sha256,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (directive_id, version, canonical_json(plan), input_sha256, actor_id, self._now()),
            )
            candidate_id = int(cursor.lastrowid)
            self._audit("candidate", str(candidate_id), "candidate.generated", actor_id, {
                "directive_id": directive_id,
                "version": version,
                "feasible": plan["feasible"],
            })
        return {"candidate_id": candidate_id, "directive_id": directive_id, "version": version,
                "feasible": plan["feasible"], "replayed": False, "state": "proposed", "plan": plan}

    def get_candidate(self, actor_id: str, candidate_id: int) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        row = self.connection.execute(
            "SELECT * FROM dr_candidates WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if row is None:
            raise NotFound("候选组合不存在")
        return {
            "candidate_id": row["candidate_id"],
            "directive_id": row["directive_id"],
            "version": row["version"],
            "state": row["state"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "confirmed_by": row["confirmed_by"],
            "confirmed_at": row["confirmed_at"],
            "plan": json.loads(row["plan_json"]),
        }

    def confirm_candidate(self, actor_id: str, candidate_id: int) -> dict[str, Any]:
        """运营确认候选：一次性冻结组合内全部资源动作。"""
        self._require(actor_id, "candidate.confirm")
        row = self.connection.execute(
            "SELECT * FROM dr_candidates WHERE candidate_id=?", (candidate_id,)
        ).fetchone()
        if row is None:
            raise NotFound("候选组合不存在")
        if row["state"] != "proposed":
            raise InvalidState("候选组合不是待确认状态")
        head = self._head(row["directive_id"])
        if row["version"] != head["current_version"]:
            raise InvalidState("候选组合不属于当前指令版本")
        version_row = self._version_row(row["directive_id"], row["version"])
        if version_row["state"] != "draft":
            raise InvalidState("该指令版本已有确认组合")
        plan = json.loads(row["plan_json"])
        if not plan["feasible"]:
            raise InvalidState("候选组合存在削减缺口，不能冻结，请调整资源或曲线后重新生成")
        now = self._now()
        plan_stage_ids = [stage["stage_id"] for stage in plan["stages"]]
        with transaction(self.connection, immediate=True):
            # 旧版本未锁定阶段的冻结动作全部作废（已锁定阶段动作保持生效）。
            locked = self._locked_stages(row["directive_id"])
            if locked:
                placeholders = ",".join("?" for _ in locked)
                self.connection.execute(
                    f"UPDATE dr_frozen_actions SET state='superseded' "
                    f"WHERE directive_id=? AND state<>'superseded' AND stage_id NOT IN ({placeholders})",
                    (row["directive_id"], *sorted(locked)),
                )
            else:
                self.connection.execute(
                    "UPDATE dr_frozen_actions SET state='superseded' "
                    "WHERE directive_id=? AND state<>'superseded'",
                    (row["directive_id"],),
                )
            self.connection.execute(
                "UPDATE dr_candidates SET state='superseded' WHERE directive_id=? AND version=? "
                "AND state='proposed' AND candidate_id<>?",
                (row["directive_id"], row["version"], candidate_id),
            )
            self.connection.execute(
                "UPDATE dr_candidates SET state='frozen',confirmed_by=?,confirmed_at=?,frozen_at=? "
                "WHERE candidate_id=?",
                (actor_id, now, now, candidate_id),
            )
            count = 0
            for stage in plan["stages"]:
                for action in stage["actions"]:
                    self.connection.execute(
                        "INSERT INTO dr_frozen_actions(candidate_id,directive_id,version,stage_id,site_id,"
                        "resource_id,kind,tenant_id,reduction_kw,state,locked_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,'locked',?)",
                        (candidate_id, row["directive_id"], row["version"], stage["stage_id"],
                         action["site_id"], action["resource_id"], action["kind"],
                         action["tenant_id"], action["reduction_kw"], now),
                    )
                    count += 1
            self.connection.execute(
                "UPDATE dr_directives SET state='confirmed' WHERE directive_id=? AND version=?",
                (row["directive_id"], row["version"]),
            )
            executing = self.connection.execute(
                "SELECT 1 FROM dr_frozen_actions WHERE directive_id=? AND state IN "
                "('dispatched','achieved','failed','restored') LIMIT 1",
                (row["directive_id"],),
            ).fetchone()
            new_state = "executing" if executing is not None else "confirmed"
            self.connection.execute(
                "UPDATE dr_directive_head SET state=?,updated_at=? WHERE directive_id=?",
                (new_state, now, row["directive_id"]),
            )
            self._audit("candidate", str(candidate_id), "candidate.frozen", actor_id, {
                "directive_id": row["directive_id"],
                "version": row["version"],
                "frozen_actions": count,
                "stages": plan_stage_ids,
            })
        return {"candidate_id": candidate_id, "directive_id": row["directive_id"],
                "version": row["version"], "state": "frozen", "frozen_actions": count,
                "directive_state": new_state}

    # ---------- 执行回执 ----------

    def ingest_receipt(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """接收执行回执：乱序/重复按事件时间归并，保护已确认终态。"""
        self._require(actor_id, "receipt.write")
        event = ReceiptEvent.from_dict(raw)
        head = self._head(event.directive_id)
        if head["state"] in {"draft", "cancelled"}:
            raise InvalidState("指令尚未确认或已取消，不能接收回执")
        duplicated = self.connection.execute(
            "SELECT event_id,applied,ignored_reason FROM dr_receipt_events "
            "WHERE directive_id=? AND source_event_id=?",
            (event.directive_id, event.source_event_id),
        ).fetchone()
        if duplicated is not None:
            return {"applied": bool(duplicated["applied"]), "duplicate": True,
                    "ignored_reason": duplicated["ignored_reason"],
                    "source_event_id": event.source_event_id}
        payload_json = canonical_json(dict(raw))
        action = self.connection.execute(
            "SELECT * FROM dr_frozen_actions WHERE directive_id=? AND stage_id=? AND resource_id=? "
            "AND state<>'superseded'",
            (event.directive_id, event.stage_id, event.resource_id),
        ).fetchone()
        applied = 0
        ignored: str | None = None
        new_state: str | None = None
        with transaction(self.connection, immediate=True):
            if head["state"] == "closed":
                ignored = "directive_closed"
            elif action is None:
                ignored = "no_frozen_action"
            elif event.event_type == action["state"]:
                ignored = "duplicate_phase"
            elif event.event_type not in ALLOWED_TRANSITIONS[action["state"]]:
                # achieved/failed/restored 为已确认终态，重复或乱序事件不得覆盖。
                ignored = (
                    "terminal_state_protected"
                    if action["state"] in {"achieved", "failed", "restored"}
                    else "phase_regression"
                )
            elif action["last_event_time"] is not None and event.event_time < action["last_event_time"]:
                # 按事件时间归并：早于已归并事件的迟到回执一律丢弃。
                ignored = "stale_event_time"
            else:
                applied = 1
                new_state = event.event_type
                observed = event.observed_reduction_kw
                self.connection.execute(
                    "UPDATE dr_frozen_actions SET state=?,last_event_time=?,last_event_type=?,"
                    "observed_reduction_kw=COALESCE(?,observed_reduction_kw) WHERE freeze_id=?",
                    (new_state, event.event_time, event.event_type,
                     None if observed is None else text(observed), action["freeze_id"]),
                )
                self.connection.execute(
                    "INSERT OR IGNORE INTO dr_stage_locks(directive_id,stage_id,locked_version,locked_at) "
                    "VALUES(?,?,?,?)",
                    (event.directive_id, event.stage_id, action["version"], self._now()),
                )
                if head["state"] == "confirmed":
                    self.connection.execute(
                        "UPDATE dr_directive_head SET state='executing',updated_at=? WHERE directive_id=?",
                        (self._now(), event.directive_id),
                    )
            self.connection.execute(
                "INSERT INTO dr_receipt_events(directive_id,source_event_id,stage_id,resource_id,"
                "event_type,event_time,observed_reduction_kw,payload_json,received_at,applied,ignored_reason) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (event.directive_id, event.source_event_id, event.stage_id, event.resource_id,
                 event.event_type, event.event_time,
                 None if event.observed_reduction_kw is None else text(event.observed_reduction_kw),
                 payload_json, self._now(), applied, ignored),
            )
            self._audit("receipt", f"{event.directive_id}:{event.source_event_id}",
                        "receipt.applied" if applied else "receipt.ignored", actor_id, {
                            "stage_id": event.stage_id,
                            "resource_id": event.resource_id,
                            "event_type": event.event_type,
                            "ignored_reason": ignored,
                        })
        return {"applied": bool(applied), "duplicate": False, "ignored_reason": ignored,
                "state": new_state, "source_event_id": event.source_event_id}

    # ---------- 关闭与取消 ----------

    def close_directive(self, actor_id: str, directive_id: str) -> dict[str, Any]:
        self._require(actor_id, "directive.close")
        head = self._head(directive_id)
        if head["state"] not in {"confirmed", "executing"}:
            raise InvalidState("只有已确认或执行中的指令可以关闭")
        open_actions = self.connection.execute(
            "SELECT count(*) AS n FROM dr_frozen_actions WHERE directive_id=? "
            "AND state IN ('locked','dispatched')",
            (directive_id,),
        ).fetchone()["n"]
        if open_actions:
            raise InvalidState("存在未完结的冻结动作，不能关闭指令")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE dr_directive_head SET state='closed',updated_at=? WHERE directive_id=?",
                (self._now(), directive_id),
            )
            self._audit("directive", directive_id, "directive.closed", actor_id, {})
        return {"directive_id": directive_id, "state": "closed"}

    def cancel_directive(self, actor_id: str, directive_id: str) -> dict[str, Any]:
        self._require(actor_id, "directive.write")
        head = self._head(directive_id)
        if head["state"] != "draft":
            raise InvalidState("只有未确认的草稿指令可以取消")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE dr_directive_head SET state='cancelled',updated_at=? WHERE directive_id=?",
                (self._now(), directive_id),
            )
            self._audit("directive", directive_id, "directive.cancelled", actor_id, {})
        return {"directive_id": directive_id, "state": "cancelled"}

    # ---------- 查询与解释 ----------

    def directive_view(self, actor_id: str, directive_id: str) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        head = self._head(directive_id)
        versions = self.connection.execute(
            "SELECT version,source_revision,state,created_at FROM dr_directives WHERE directive_id=? "
            "ORDER BY version",
            (directive_id,),
        ).fetchall()
        current = self._version_row(directive_id, head["current_version"])
        locked = self._locked_stages(directive_id)
        return {
            "directive_id": directive_id,
            "state": head["state"],
            "current_version": head["current_version"],
            "definition": json.loads(current["definition_json"]),
            "locked_stages": sorted(locked),
            "versions": [dict(row) for row in versions],
        }

    def list_candidates(self, actor_id: str, directive_id: str, version: int) -> dict[str, Any]:
        self._require(actor_id, "report.read")
        rows = self.connection.execute(
            "SELECT candidate_id,state,created_by,created_at,confirmed_by,confirmed_at "
            "FROM dr_candidates WHERE directive_id=? AND version=? ORDER BY candidate_id",
            (directive_id, version),
        ).fetchall()
        return {"directive_id": directive_id, "version": version,
                "candidates": [dict(row) for row in rows]}

    def directive_report(self, actor_id: str, directive_id: str) -> dict[str, Any]:
        """解释实际削减量、未达标原因、各租户影响和恢复计划。"""
        self._require(actor_id, "report.read")
        head = self._head(directive_id)
        version_row = self._version_row(directive_id, head["current_version"])
        definition = json.loads(version_row["definition_json"])
        stage_defs = {item["stage_id"]: item for item in definition["curve"]}
        stage_hours = {
            stage_id: Decimal(
                (parse_utc(item["ends_at"]) - parse_utc(item["starts_at"])).total_seconds()
            ) / Decimal(3600)
            for stage_id, item in stage_defs.items()
        }
        actions = self.connection.execute(
            "SELECT * FROM dr_frozen_actions WHERE directive_id=? AND state<>'superseded' "
            "ORDER BY stage_id,resource_id",
            (directive_id,),
        ).fetchall()
        by_stage: dict[str, list[sqlite3.Row]] = {}
        for action in actions:
            by_stage.setdefault(action["stage_id"], []).append(action)

        # 各阶段规划期短口原因：取覆盖该阶段的最新冻结候选（支持跨版本修订）。
        frozen_rows = self.connection.execute(
            "SELECT candidate_id,version,plan_json FROM dr_candidates WHERE directive_id=? "
            "AND state='frozen' ORDER BY candidate_id",
            (directive_id,),
        ).fetchall()
        planned_reasons: dict[str, list[dict[str, Any]]] = {}
        for frozen in frozen_rows:
            for stage in json.loads(frozen["plan_json"])["stages"]:
                planned_reasons[stage["stage_id"]] = stage["reasons"]

        stage_rows = []
        total_target_kwh = ZERO
        total_planned_kwh = ZERO
        total_actual_kwh = ZERO
        any_reported = False
        for stage_id in sorted(stage_defs, key=lambda s: stage_defs[s]["starts_at"]):
            stage_def = stage_defs[stage_id]
            target = Decimal(stage_def["target_reduction_kw"])
            stage_actions = by_stage.get(stage_id, [])
            planned = sum((Decimal(a["reduction_kw"]) for a in stage_actions), ZERO)
            # 已回执动作按实测计入；尚未回执（locked/dispatched）暂按冻结计划估算。
            actual_parts: list[Decimal] = []
            reported_any = False
            for action in stage_actions:
                if action["state"] in {"achieved", "restored"} and action["observed_reduction_kw"] is not None:
                    actual_parts.append(Decimal(action["observed_reduction_kw"]))
                    reported_any = True
                elif action["state"] == "failed":
                    actual_parts.append(ZERO)
                    reported_any = True
                else:
                    actual_parts.append(Decimal(action["reduction_kw"]))
            actual = sum(actual_parts, ZERO) if reported_any else None
            reference = actual if actual is not None else planned
            gap = q(max(ZERO, target - reference))
            states = {a["state"] for a in stage_actions}
            if not stage_actions:
                stage_state = "unplanned"
            elif states <= {"locked"}:
                stage_state = "pending"
            elif states == {"restored"}:
                stage_state = "completed"
            elif "failed" in states and states <= {"failed", "restored"}:
                stage_state = "completed_with_failures"
            else:
                stage_state = "executing"
            reasons: list[dict[str, Any]] = []
            if gap > ZERO:
                reasons.extend(planned_reasons.get(stage_id, []))
                failed_refs = [a["resource_id"] for a in stage_actions if a["state"] == "failed"]
                if failed_refs:
                    failed_kw = sum((Decimal(a["reduction_kw"]) for a in stage_actions
                                     if a["state"] == "failed"), ZERO)
                    reasons.append({
                        "code": "action_failed",
                        "message": "冻结动作执行失败",
                        "blocked_kw": text(q(failed_kw)),
                        "resource_refs": sorted(failed_refs),
                    })
                under = [
                    a for a in stage_actions
                    if a["state"] in {"achieved", "restored"}
                    and a["observed_reduction_kw"] is not None
                    and Decimal(a["observed_reduction_kw"]) < Decimal(a["reduction_kw"])
                ]
                if under:
                    shortfall = sum(
                        (Decimal(a["reduction_kw"]) - Decimal(a["observed_reduction_kw"]) for a in under),
                        ZERO,
                    )
                    reasons.append({
                        "code": "under_delivery",
                        "message": "实际削减低于冻结计划",
                        "blocked_kw": text(q(shortfall)),
                        "resource_refs": sorted(a["resource_id"] for a in under),
                    })
            hours = stage_hours[stage_id]
            total_target_kwh = q(total_target_kwh + target * hours)
            total_planned_kwh = q(total_planned_kwh + planned * hours)
            if actual is not None:
                total_actual_kwh = q(total_actual_kwh + actual * hours)
                any_reported = True
            stage_rows.append({
                "stage_id": stage_id,
                "starts_at": stage_def["starts_at"],
                "ends_at": stage_def["ends_at"],
                "state": stage_state,
                "target_reduction_kw": text(target),
                "planned_reduction_kw": text(q(planned)),
                "actual_reduction_kw": None if actual is None else text(q(actual)),
                "gap_kw": text(gap),
                "met": gap <= ZERO,
                "actions": [
                    {
                        "resource_id": a["resource_id"],
                        "kind": a["kind"],
                        "tenant_id": a["tenant_id"],
                        "site_id": a["site_id"],
                        "planned_kw": a["reduction_kw"],
                        "state": a["state"],
                        "observed_reduction_kw": a["observed_reduction_kw"],
                        "last_event_time": a["last_event_time"],
                    }
                    for a in stage_actions
                ],
                "shortfall_reasons": reasons,
            })

        # 租户影响：窗口内峰值削减与涉及资源。
        peak: dict[tuple[str, str], Decimal] = {}
        delayed: dict[tuple[str, str], set[str]] = {}
        capped: dict[tuple[str, str], set[str]] = {}
        for stage_id, stage_actions in by_stage.items():
            per_tenant: dict[tuple[str, str], Decimal] = {}
            for a in stage_actions:
                if a["tenant_id"] is None:
                    continue
                key = (a["site_id"], a["tenant_id"])
                per_tenant[key] = per_tenant.get(key, ZERO) + Decimal(a["reduction_kw"])
                if a["kind"] == "job":
                    delayed.setdefault(key, set()).add(a["resource_id"])
                elif a["kind"] == "cap":
                    capped.setdefault(key, set()).add(a["resource_id"])
            for key, value in per_tenant.items():
                peak[key] = max(peak.get(key, ZERO), value)
        guarantees = self._guarantees(directive_id, head["current_version"])
        tenant_rows = [
            {
                "site_id": key[0],
                "tenant_id": key[1],
                "protected_min_compute_kw": text(guarantees.get(key, ZERO)),
                "peak_reduced_kw": text(q(peak.get(key, ZERO))),
                "delayed_jobs": sorted(delayed.get(key, set())),
                "capped_resources": sorted(capped.get(key, set())),
            }
            for key in sorted(set(peak) | set(delayed) | set(capped))
        ]

        # 恢复计划：由生效冻结动作与当前版本资源属性推导。
        resources = {r["resource_id"]: r for r in self._resources(directive_id, head["current_version"])}
        recovery_jobs: dict[str, dict[str, Any]] = {}
        recovery_caps: dict[str, dict[str, Any]] = {}
        storage_kwh: dict[str, Decimal] = {}
        for a in actions:
            resource = resources.get(a["resource_id"])
            if resource is None:
                continue
            detail = resource["detail"]
            if a["kind"] == "job":
                recovery_jobs[a["resource_id"]] = {
                    "resource_id": a["resource_id"],
                    "site_id": a["site_id"],
                    "tenant_id": a["tenant_id"],
                    "resume_at": definition["window_end"],
                    "remaining_duration_minutes": str(detail["duration_minutes"]),
                }
            elif a["kind"] == "cap":
                recovery_caps[a["resource_id"]] = {
                    "resource_id": a["resource_id"],
                    "site_id": a["site_id"],
                    "tenant_id": a["tenant_id"],
                    "release_cap_at": definition["window_end"],
                    "restore_to_kw": str(detail["current_kw"]),
                }
            else:
                hours = stage_hours.get(a["stage_id"], ZERO)
                storage_kwh[a["resource_id"]] = q(
                    storage_kwh.get(a["resource_id"], ZERO) + Decimal(a["reduction_kw"]) * hours
                )
        recovery_storage = []
        for resource_id in sorted(storage_kwh):
            resource = resources[resource_id]
            discharged = storage_kwh[resource_id]
            recharge_kw = Decimal(str(resource["detail"]["recharge_kw"]))
            recharge_hours = discharged / recharge_kw if recharge_kw > ZERO else ZERO
            ready_seconds = int(q(recharge_hours * Decimal(3600)).to_integral_value())
            ready_at = (
                parse_utc(definition["window_end"]) + timedelta(seconds=ready_seconds)
            ).isoformat().replace("+00:00", "Z")
            recovery_storage.append({
                "resource_id": resource_id,
                "site_id": resource["site_id"],
                "discharged_kwh": text(discharged),
                "recharge_kw": str(resource["detail"]["recharge_kw"]),
                "recharge_start_at": definition["window_end"],
                "recharge_ready_at": ready_at,
                "recover_by_at": definition["recover_by_at"],
                "within_recovery_window": parse_utc(ready_at) <= parse_utc(definition["recover_by_at"]),
            })

        return {
            "directive_id": directive_id,
            "state": head["state"],
            "current_version": head["current_version"],
            "window_start": definition["window_start"],
            "window_end": definition["window_end"],
            "recover_by_at": definition["recover_by_at"],
            "stages": stage_rows,
            "totals": {
                "target_kwh": text(total_target_kwh),
                "planned_kwh": text(total_planned_kwh),
                "actual_kwh": text(total_actual_kwh) if reported_any else None,
            },
            "tenant_impact": tenant_rows,
            "recovery_plan": {
                "window_end": definition["window_end"],
                "recover_by_at": definition["recover_by_at"],
                "jobs": [recovery_jobs[key] for key in sorted(recovery_jobs)],
                "caps": [recovery_caps[key] for key in sorted(recovery_caps)],
                "storage": recovery_storage,
            },
        }

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM dr_audit_events ORDER BY event_id").fetchall()
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
