"""需求响应协同服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dr_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('operator','approver','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

-- 需求响应指令：同一指令以版本号修订，只保留各版本不可变快照。
CREATE TABLE IF NOT EXISTS dr_directives (
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    source_revision TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    recover_by_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('draft','confirmed')),
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    supersedes_version INTEGER,
    created_by TEXT NOT NULL REFERENCES dr_users(user_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY(directive_id, version)
);

-- 指令头：当前生效版本与整体生命周期状态（版本行本身不可变）。
CREATE TABLE IF NOT EXISTS dr_directive_head (
    directive_id TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('draft','confirmed','executing','closed','cancelled')),
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_directives_state ON dr_directives(state, window_start);

-- 站点基线（按版本快照保存）。
CREATE TABLE IF NOT EXISTS dr_baselines (
    baseline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    site_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    baseline_kw TEXT NOT NULL,
    UNIQUE(directive_id, version, site_id, stage_id),
    FOREIGN KEY(directive_id, version) REFERENCES dr_directives(directive_id, version)
);

-- 租户最低算力保底。
CREATE TABLE IF NOT EXISTS dr_tenant_guarantees (
    guarantee_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    site_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    min_compute_kw TEXT NOT NULL,
    UNIQUE(directive_id, version, site_id, tenant_id),
    FOREIGN KEY(directive_id, version) REFERENCES dr_directives(directive_id, version)
);

-- 机房可调资源（按版本快照保存，修订只复制未锁阶段仍可参与的资源快照）。
CREATE TABLE IF NOT EXISTS dr_resources (
    resource_id TEXT NOT NULL,
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    site_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('job','cap','storage')),
    tenant_id TEXT,
    detail_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL REFERENCES dr_users(user_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY(resource_id, directive_id, version)
);

CREATE INDEX IF NOT EXISTS idx_dr_resources_lookup
ON dr_resources(directive_id, version, site_id, kind);

-- 候选组合（可多次生成，确认时一次性冻结其中一个）。
CREATE TABLE IF NOT EXISTS dr_candidates (
    candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    plan_json TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'proposed'
        CHECK(state IN ('proposed','frozen','superseded')),
    created_by TEXT NOT NULL REFERENCES dr_users(user_id),
    created_at TEXT NOT NULL,
    confirmed_by TEXT REFERENCES dr_users(user_id),
    confirmed_at TEXT,
    frozen_at TEXT,
    UNIQUE(directive_id, version, input_sha256),
    FOREIGN KEY(directive_id, version) REFERENCES dr_directives(directive_id, version)
);

-- 冻结动作：确认候选时按 (阶段,资源) 一次性写入，构成不可变执行计划。
-- 指令修订只影响未锁定阶段：被取代版本的冻结动作置为 superseded，
-- 通过部分唯一索引保证同一 (指令,阶段,资源) 至多一条生效记录。
CREATE TABLE IF NOT EXISTS dr_frozen_actions (
    freeze_id INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id INTEGER NOT NULL REFERENCES dr_candidates(candidate_id),
    directive_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    stage_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    tenant_id TEXT,
    reduction_kw TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'locked'
        CHECK(state IN ('locked','dispatched','achieved','failed','restored','superseded')),
    observed_reduction_kw TEXT,
    last_event_time TEXT,
    last_event_type TEXT,
    locked_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dr_frozen_active
ON dr_frozen_actions(directive_id, stage_id, resource_id) WHERE state <> 'superseded';

CREATE INDEX IF NOT EXISTS idx_dr_frozen_stage
ON dr_frozen_actions(directive_id, stage_id, state);

-- 阶段锁：指令修订只能影响未锁定阶段；确认时全部阶段锁定。
CREATE TABLE IF NOT EXISTS dr_stage_locks (
    directive_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    locked_version INTEGER NOT NULL,
    locked_at TEXT NOT NULL,
    PRIMARY KEY(directive_id, stage_id)
);

-- 回执原始事件（source_event_id 幂等去重）。
CREATE TABLE IF NOT EXISTS dr_receipt_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directive_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    event_type TEXT NOT NULL
        CHECK(event_type IN ('dispatched','achieved','failed','restored')),
    event_time TEXT NOT NULL,
    observed_reduction_kw TEXT,
    payload_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0 CHECK(applied IN (0,1)),
    ignored_reason TEXT,
    UNIQUE(directive_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_dr_receipts_target
ON dr_receipt_events(directive_id, stage_id, resource_id, event_time);

-- 幂等结果。
CREATE TABLE IF NOT EXISTS dr_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

-- 哈希链审计事件。
CREATE TABLE IF NOT EXISTS dr_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_audit_entity
ON dr_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    connection = sqlite3.connect(
        str(path), isolation_level=None, timeout=10, check_same_thread=check_same_thread
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
