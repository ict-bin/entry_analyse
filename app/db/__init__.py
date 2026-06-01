"""Database engine and session management."""

from __future__ import annotations

import logging
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from .models import Base

logger = logging.getLogger("ea.db")

_engine = None
_SessionLocal = None

_MIGRATIONS = [
    # Add stages_json for real-time stage event tracking (added 2026-05)
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN stages_json JSON NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN latest_abnormal_reason_json JSON NULL",
    # Add task_config_json for per-task overrides and resume params (added 2026-05)
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN task_config_json JSON NULL",
    # Add source_path for separate source code root directory (added 2026-05)
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN source_path VARCHAR(1024) NULL",
    # Add module_name for explicit module selection (added 2026-05)
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN module_name VARCHAR(255) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN task_origin_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_project_id VARCHAR(100) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_task_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_task_type VARCHAR(32) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_stage_name VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_stage_item_id VARCHAR(64) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN parent_stage_item_key VARCHAR(255) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN owner_pod VARCHAR(128) NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN lease_expires_at DATETIME NULL",
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT FALSE",
    # Add owner_pod_ip for instant cancel notification without DNS lookup (added 2026-05)
    "ALTER TABLE secflow_app_ea_tasks ADD COLUMN owner_pod_ip VARCHAR(64) NULL",
    "CREATE INDEX ix_ea_tasks_project_deleted_created_id ON secflow_app_ea_tasks (project_id, is_deleted, created_at, id)",
    "CREATE INDEX ix_ea_tasks_project_created_id ON secflow_app_ea_tasks (project_id, created_at, id)",
    "CREATE INDEX ix_ea_tasks_project_deleted_status_created_id ON secflow_app_ea_tasks (project_id, is_deleted, status, created_at, id)",
    "CREATE INDEX ix_ea_tasks_project_deleted_status_lease_id ON secflow_app_ea_tasks (project_id, is_deleted, status, lease_expires_at, id)",
    "CREATE INDEX ix_ea_tasks_parent_stage_item_id_lookup ON secflow_app_ea_tasks (project_id, is_deleted, parent_task_id, parent_stage_name, parent_stage_item_id, created_at, id)",
    "CREATE INDEX ix_ea_tasks_parent_stage_item_key_lookup ON secflow_app_ea_tasks (project_id, is_deleted, parent_task_id, parent_stage_name, parent_stage_item_key, created_at, id)",
    # 列表默认按 updated_at DESC 排序，加入覆盖索引消除 filesort
    "CREATE INDEX ix_ea_tasks_project_deleted_updated_id ON secflow_app_ea_tasks (project_id, is_deleted, updated_at, id)",
    "CREATE INDEX ix_ea_stage_result_task_stage_role_attempt ON secflow_app_ea_stage_result_index (task_id, stage_key, role_kind, attempt)",
    "CREATE INDEX ix_ea_stage_result_task_func_stage ON secflow_app_ea_stage_result_index (task_id, func_hash, stage_key)",
    "CREATE INDEX ix_ea_stage_result_task_file_stage ON secflow_app_ea_stage_result_index (task_id, file_hash, stage_key)",
    """
    CREATE TABLE secflow_app_ea_worker_slots (
        id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
        worker_id VARCHAR(128) NOT NULL UNIQUE,
        pod_name VARCHAR(128) NOT NULL,
        pod_ip VARCHAR(64) NULL,
        max_concurrent_tasks INTEGER NOT NULL DEFAULT 1,
        agent_process_limit INTEGER NOT NULL DEFAULT 0,
        agent_process_in_use INTEGER NOT NULL DEFAULT 0,
        agent_process_available INTEGER NOT NULL DEFAULT 0,
        agent_waiting_requests INTEGER NOT NULL DEFAULT 0,
        agent_waiting_tasks INTEGER NOT NULL DEFAULT 0,
        agent_queue_oldest_wait_seconds DOUBLE NOT NULL DEFAULT 0,
        agent_rss_total_bytes BIGINT NOT NULL DEFAULT 0,
        agent_rss_max_bytes BIGINT NOT NULL DEFAULT 0,
        agent_snapshot_at DATETIME NULL,
        last_seen_status VARCHAR(32) NOT NULL DEFAULT 'running',
        heartbeat_error TEXT NULL,
        heartbeat_duration_ms DOUBLE NULL,
        heartbeat_failure_count INTEGER NOT NULL DEFAULT 0,
        last_heartbeat_at DATETIME NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
    )
    """,
    "CREATE INDEX ix_ea_worker_slots_pod_name ON secflow_app_ea_worker_slots (pod_name)",
    "CREATE INDEX ix_ea_worker_slots_last_heartbeat ON secflow_app_ea_worker_slots (last_heartbeat_at)",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_process_limit INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_process_in_use INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_process_available INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_waiting_requests INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_waiting_tasks INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_queue_oldest_wait_seconds DOUBLE NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_rss_total_bytes BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_rss_max_bytes BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN agent_snapshot_at DATETIME NULL",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN heartbeat_error TEXT NULL",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN heartbeat_duration_ms DOUBLE NULL",
    "ALTER TABLE secflow_app_ea_worker_slots ADD COLUMN heartbeat_failure_count INTEGER NOT NULL DEFAULT 0",
    """
    CREATE TABLE secflow_app_ea_dispatch_leases (
        id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
        project_id VARCHAR(100) NOT NULL UNIQUE,
        lease_owner VARCHAR(128) NOT NULL,
        lease_token VARCHAR(64) NOT NULL,
        operation VARCHAR(32) NOT NULL DEFAULT 'dispatch',
        lease_expires_at DATETIME NOT NULL,
        heartbeat_at DATETIME NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL
    )
    """,
    "CREATE INDEX ix_ea_dispatch_leases_owner ON secflow_app_ea_dispatch_leases (lease_owner)",
    "CREATE INDEX ix_ea_dispatch_leases_expires ON secflow_app_ea_dispatch_leases (lease_expires_at)",
    """
    CREATE TABLE secflow_app_ea_task_event (
        id VARCHAR(32) NOT NULL PRIMARY KEY,
        task_id VARCHAR(64) NOT NULL,
        project_id VARCHAR(100) NOT NULL,
        source VARCHAR(32) NOT NULL DEFAULT 'entry_analyse',
        level VARCHAR(16) NOT NULL DEFAULT 'info',
        event_type VARCHAR(64) NOT NULL,
        stage_key VARCHAR(64) NULL,
        file_hash VARCHAR(64) NULL,
        func_hash VARCHAR(64) NULL,
        file_path VARCHAR(1024) NULL,
        function_name VARCHAR(255) NULL,
        attempt INTEGER NULL,
        status VARCHAR(32) NULL,
        message TEXT NOT NULL,
        payload_json TEXT NULL,
        dedupe_key VARCHAR(255) NOT NULL UNIQUE,
        created_at DATETIME NOT NULL
    )
    """,
    "CREATE INDEX ix_ea_task_event_task_created ON secflow_app_ea_task_event (task_id, created_at)",
    "CREATE INDEX ix_ea_task_event_task_event_type ON secflow_app_ea_task_event (task_id, event_type, created_at)",
    "CREATE INDEX ix_ea_task_event_task_stage_key ON secflow_app_ea_task_event (task_id, stage_key, created_at)",
]


def _run_migrations(engine) -> None:
    """Apply additive schema migrations; silently skips already-applied ones."""
    with engine.connect() as conn:
        for stmt in _MIGRATIONS:
            try:
                conn.execute(text(stmt))
                conn.commit()
                logger.info("Migration applied: %s", stmt[:60])
            except Exception:
                conn.rollback()


def init_db(db_url: str, pool_size: int = 5, max_overflow: int = 10) -> None:
    """Initialize the database engine and create tables."""
    global _engine, _SessionLocal
    _engine = create_engine(
        db_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    Base.metadata.create_all(bind=_engine)
    _run_migrations(_engine)
    logger.info("Database initialized")


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that provides a DB session."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    db: Session = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
