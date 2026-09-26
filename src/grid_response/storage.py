"""需求响应协同模块的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS grid_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','operator','dispatcher','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dr_sites (
    site_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL,
    baseline_kw TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dr_tenant_floors (
    site_id TEXT NOT NULL REFERENCES dr_sites(site_id),
    tenant_id TEXT NOT NULL,
    current_load_kw TEXT NOT NULL,
    min_capacity_kw TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    PRIMARY KEY(site_id, tenant_id)
);

CREATE TABLE IF NOT EXISTS dr_resources (
    resource_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES dr_sites(site_id),
    kind TEXT NOT NULL CHECK(kind IN ('job_delay','power_cap','storage')),
    tenant_id TEXT,
    adjustable_kw TEXT NOT NULL,
    ramp_kw_per_minute TEXT NOT NULL,
    max_delay_minutes INTEGER,
    energy_kwh TEXT,
    protected INTEGER NOT NULL DEFAULT 0 CHECK(protected IN (0,1)),
    state TEXT NOT NULL DEFAULT 'available' CHECK(state IN ('available','frozen','retired')),
    frozen_event_id TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_resources_site
ON dr_resources(site_id, state);

CREATE TABLE IF NOT EXISTS dr_events (
    event_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version > 0),
    site_id TEXT NOT NULL REFERENCES dr_sites(site_id),
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    recovery_minutes INTEGER NOT NULL,
    baseline_kw TEXT NOT NULL,
    curve_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('registered','planned','confirmed','closed')),
    created_by TEXT NOT NULL REFERENCES grid_users(user_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY(event_id, version)
);

CREATE TABLE IF NOT EXISTS dr_phases (
    event_id TEXT NOT NULL,
    phase_seq INTEGER NOT NULL,
    version_introduced INTEGER NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    target_reduction_kw TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','executing','executed','superseded')),
    PRIMARY KEY(event_id, phase_seq),
    FOREIGN KEY(event_id, version_introduced) REFERENCES dr_events(event_id, version)
);

CREATE INDEX IF NOT EXISTS idx_dr_phases_event
ON dr_phases(event_id, state);

CREATE TABLE IF NOT EXISTS dr_candidate_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    input_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES grid_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(event_id, version, input_sha256)
);

CREATE TABLE IF NOT EXISTS dr_candidates (
    candidate_id TEXT PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES dr_candidate_runs(run_id),
    event_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    actions_json TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    feasible INTEGER NOT NULL CHECK(feasible IN (0,1)),
    state TEXT NOT NULL DEFAULT 'proposed' CHECK(state IN ('proposed','confirmed','superseded','cancelled')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_candidates_event
ON dr_candidates(event_id, version, state);

CREATE TABLE IF NOT EXISTS dr_actions (
    action_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    phase_seq INTEGER NOT NULL,
    candidate_id TEXT NOT NULL REFERENCES dr_candidates(candidate_id),
    resource_id TEXT NOT NULL REFERENCES dr_resources(resource_id),
    kind TEXT NOT NULL,
    tenant_id TEXT,
    planned_kw TEXT NOT NULL,
    actual_kw TEXT NOT NULL DEFAULT '0',
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'frozen'
        CHECK(state IN ('frozen','executing','achieved','partial','failed','recovering','recovered','cancelled')),
    last_event_at TEXT,
    last_receipt_id TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dr_actions_event
ON dr_actions(event_id, phase_seq);

CREATE TABLE IF NOT EXISTS dr_receipts (
    receipt_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    action_id TEXT NOT NULL REFERENCES dr_actions(action_id),
    status TEXT NOT NULL
        CHECK(status IN ('started','achieved','partial','failed','recovery_started','recovered')),
    measured_kw TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    applied INTEGER NOT NULL CHECK(applied IN (0,1)),
    note TEXT NOT NULL DEFAULT '',
    recorded_by TEXT NOT NULL REFERENCES grid_users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_dr_receipts_action
ON dr_receipts(action_id, occurred_at);

CREATE TABLE IF NOT EXISTS grid_audit_events (
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

CREATE INDEX IF NOT EXISTS idx_grid_audit_entity
ON grid_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    # ThreadingHTTPServer 在工作线程中处理请求，连接需允许跨线程并配合应用层锁串行化。
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10, check_same_thread=False)
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


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
