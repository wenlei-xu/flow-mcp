"""gflow studio local control plane.

This adapter intentionally lives outside flow2api-omni. It exposes a small,
browser-friendly API while delegating authentication, profiles, Flow projects,
generation and local output handling to gflow-cli.
"""

from __future__ import annotations

import asyncio
import base64
import csv
from collections import deque
from contextlib import closing
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
import mimetypes
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote, urlencode, urlparse

# The checkout is intentionally kept next to this prototype. In production,
# set GFLOW_CLI_SRC explicitly and keep the same import boundary.
HERE = Path(__file__).resolve().parent
GFLOW_SRC = Path(os.environ.get("GFLOW_CLI_SRC", str(HERE.parent / "gflow-cli" / "src")))
if str(GFLOW_SRC) not in sys.path:
    sys.path.insert(0, str(GFLOW_SRC))

from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402
from starlette.background import BackgroundTask  # noqa: E402

from gflow_cli import profile_store  # noqa: E402
from gflow_cli.auth import status as profile_status  # noqa: E402
from gflow_cli.api.client import FlowApiClient  # noqa: E402
from gflow_cli.config import get_settings  # noqa: E402
from gflow_cli.data.queries import list_errors, list_images, list_videos  # noqa: E402
from gflow_cli.mcp.tools import (  # noqa: E402
    configure_rate_limiter,
    gflow_generate_image,
    gflow_generate_video,
    gflow_get_credits,
    gflow_list_projects,
    reset_generation_progress_callback,
    set_generation_progress_callback,
)
from gflow_cli.cli_models import build_catalog  # noqa: E402


DIST_DIR = HERE / "dist"
CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "GFLOW_STUDIO_CORS_ORIGINS",
        "http://127.0.0.1:8090,http://localhost:8090",
    ).split(",")
    if origin.strip()
]
app = FastAPI(title="gflow studio", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if (DIST_DIR / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="react-assets")
PROFILE_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
PROJECT_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
UPLOAD_DIR = Path(os.environ.get("GFLOW_STUDIO_UPLOAD_DIR", str(HERE / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.environ.get("GFLOW_STUDIO_DB_PATH", str(HERE / "gflow-studio.db")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
TASKS: dict[str, dict[str, Any]] = {}
TASK_HANDLES: dict[str, asyncio.Task[Any]] = {}
PROFILE_LOCKS: dict[str, asyncio.Lock] = {}
POOL_CURSOR = 0
QUEUE_WAKE = asyncio.Event()
QUEUE_STOP = False
QUEUE_DISPATCHER: asyncio.Task[Any] | None = None
WORKER_LEASE_HEARTBEAT: asyncio.Task[Any] | None = None
QUEUE_WORKER_ID = f"studio-{uuid.uuid4().hex[:10]}"
SHUTTING_DOWN = False
WORKER_LEASE_SECONDS = max(15, int(os.environ.get("GFLOW_STUDIO_WORKER_LEASE_SECONDS", "45")))
QUEUE_MAX_WORKERS = max(1, int(os.environ.get("GFLOW_STUDIO_QUEUE_WORKERS", "4")))
IMAGE_MAX_WORKERS = max(1, int(os.environ.get("GFLOW_STUDIO_IMAGE_WORKERS", "8")))
VIDEO_MAX_WORKERS = max(1, int(os.environ.get("GFLOW_STUDIO_VIDEO_WORKERS", "4")))
GENERATION_TIMEOUT_SECONDS = max(60, int(os.environ.get("GFLOW_STUDIO_GENERATION_TIMEOUT_SECONDS", "1800")))
MEDIA_URL_TTL_SECONDS = max(300, int(os.environ.get("GFLOW_STUDIO_MEDIA_TTL_SECONDS", "86400")))
PUBLIC_BASE_URL = os.environ.get("GFLOW_STUDIO_PUBLIC_BASE_URL", "").rstrip("/")
STUDIO_API_KEY = os.environ.get("GFLOW_STUDIO_API_KEY", "").strip()
STUDIO_ADMIN_USER = os.environ.get("GFLOW_STUDIO_ADMIN_USER", "admin").strip()
STUDIO_ADMIN_PASSWORD = os.environ.get("GFLOW_STUDIO_ADMIN_PASSWORD", "").strip()
STUDIO_SESSION_SECRET = (
    os.environ.get("GFLOW_STUDIO_SESSION_SECRET", "").strip()
    or STUDIO_API_KEY
    or "local-development-session-secret"
)
AUTH_REQUIRED = bool(STUDIO_API_KEY or STUDIO_ADMIN_PASSWORD or os.environ.get("GFLOW_STUDIO_REQUIRE_AUTH") == "1")
STUDIO_ROLE = os.environ.get("GFLOW_STUDIO_ROLE", "all").strip().lower()
MEDIA_SIGNING_SECRET = (
    os.environ.get("GFLOW_STUDIO_MEDIA_SIGNING_SECRET", "").strip()
    or STUDIO_SESSION_SECRET
)
MCP_UPSTREAM_URL = os.environ.get("GFLOW_STUDIO_MCP_UPSTREAM_URL", "http://127.0.0.1:18080/mcp").rstrip("/")
MCP_UPSTREAM_AUTH = os.environ.get("GFLOW_STUDIO_MCP_UPSTREAM_AUTH", "").strip()
ASSET_MAX_BYTES = max(1_048_576, int(os.environ.get("GFLOW_STUDIO_ASSET_MAX_BYTES", str(25 * 1024 * 1024))))
ASSET_MAX_TOTAL_BYTES = max(ASSET_MAX_BYTES, int(os.environ.get("GFLOW_STUDIO_ASSET_MAX_TOTAL_BYTES", str(2 * 1024 * 1024 * 1024))))
ASSET_RETENTION_SECONDS = max(3600, int(os.environ.get("GFLOW_STUDIO_ASSET_RETENTION_SECONDS", str(7 * 86400))))
RATE_LIMIT_PER_MINUTE = max(30, int(os.environ.get("GFLOW_STUDIO_RATE_LIMIT_PER_MINUTE", "180")))
RATE_BUCKETS: dict[str, deque[float]] = {}
GENERATION_RATE_CAPACITY = 8
GENERATION_RATE_REFILL_SECONDS = 20.0


def _is_loopback(request: Request) -> bool:
    return request.client is not None and request.client.host in {"127.0.0.1", "::1", "localhost"}


def _valid_session(value: str | None) -> bool:
    if not value:
        return False
    try:
        encoded_user, encoded_expiry, encoded_signature = value.split(".", 2)
        payload = f"{encoded_user}.{encoded_expiry}"
        expected = hmac.new(STUDIO_SESSION_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return (
            hmac.compare_digest(encoded_signature, expected)
            and int(encoded_expiry) >= int(time.time())
            and encoded_user == quote(STUDIO_ADMIN_USER, safe="")
        )
    except (TypeError, ValueError):
        return False


def _valid_request_auth(request: Request) -> bool:
    supplied = request.headers.get("x-api-key", "")
    authorization = request.headers.get("authorization", "")
    if authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    return bool(STUDIO_API_KEY) and hmac.compare_digest(supplied, STUDIO_API_KEY)


@app.middleware("http")
async def studio_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path.startswith(("/api/", "/v1/", "/models/", "/v1beta/", "/mcp")) and path not in {"/api/health", "/api/auth/status"}:
        key = request.client.host if request.client else "unknown"
        now = time.monotonic()
        bucket = RATE_BUCKETS.setdefault(key, deque())
        while bucket and now - bucket[0] >= 60:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT_PER_MINUTE:
            return JSONResponse(status_code=429, content={"detail": "请求过于频繁，请稍后重试"}, headers={"Retry-After": "60"})
        bucket.append(now)
    public_paths = {"/api/health", "/api/auth/status", "/api/auth/login", "/api/media/signed"}
    if not path.startswith("/api/") or path in public_paths:
        return await call_next(request)
    if not AUTH_REQUIRED and _is_loopback(request):
        return await call_next(request)
    if not AUTH_REQUIRED:
        return JSONResponse(
            status_code=503,
            content={"detail": "未配置 GFLOW_STUDIO_API_KEY 或 GFLOW_STUDIO_ADMIN_PASSWORD，拒绝非本机访问"},
        )
    if _valid_request_auth(request) or _valid_session(request.cookies.get("gflow_session")):
        return await call_next(request)
    return JSONResponse(
        status_code=401,
        content={"detail": "需要 gflow studio 管理认证"},
        headers={"WWW-Authenticate": "Bearer"},
    )


def _init_db() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS studio_tasks (
                id TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                task_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                idempotency_key TEXT,
                request_hash TEXT,
                available_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                timeout_seconds INTEGER NOT NULL DEFAULT 1800
            )""",
        )
        # Existing prototype databases predate the queue index columns. Keep
        # the migration deliberately additive so an upgrade never deletes a
        # task or profile.
        existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(studio_tasks)").fetchall()}
        for column, definition in {
            "status": "TEXT NOT NULL DEFAULT 'queued'",
            "idempotency_key": "TEXT",
            "request_hash": "TEXT",
            "available_at": "TEXT",
            "attempts": "INTEGER NOT NULL DEFAULT 0",
            "timeout_seconds": "INTEGER NOT NULL DEFAULT 1800",
        }.items():
            if column not in existing_columns:
                conn.execute(f"ALTER TABLE studio_tasks ADD COLUMN {column} {definition}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_studio_tasks_queue ON studio_tasks(status, available_at, updated_at)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_studio_tasks_idempotency ON studio_tasks(idempotency_key) WHERE idempotency_key IS NOT NULL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS studio_worker_lease (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                worker_id TEXT NOT NULL,
                pid INTEGER NOT NULL,
                heartbeat_at REAL NOT NULL
            )""",
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS profile_settings (
                profile_name TEXT PRIMARY KEY,
                remark TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                image_concurrency INTEGER NOT NULL DEFAULT 1,
                video_concurrency INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            )""",
        )
        profile_setting_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(profile_settings)").fetchall()
        }
        for column, definition in {
            "image_concurrency": "INTEGER NOT NULL DEFAULT 1",
            "video_concurrency": "INTEGER NOT NULL DEFAULT 1",
        }.items():
            if column not in profile_setting_columns:
                conn.execute(f"ALTER TABLE profile_settings ADD COLUMN {column} {definition}")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS profile_projects (
                profile_name TEXT NOT NULL,
                project_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                is_default INTEGER NOT NULL DEFAULT 0,
                use_count INTEGER NOT NULL DEFAULT 0,
                last_used_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(profile_name, project_id)
            )""",
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS profile_health (
                profile_name TEXT PRIMARY KEY,
                auth_state TEXT NOT NULL DEFAULT 'unknown',
                last_checked_at TEXT,
                last_success_at TEXT,
                last_failure_at TEXT,
                last_error TEXT NOT NULL DEFAULT '',
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            )""",
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS studio_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""",
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS studio_assets (
                asset_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                local_path TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_accessed_at TEXT,
                expires_at TEXT NOT NULL
            )""",
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_studio_assets_sha256 ON studio_assets(sha256)")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS studio_events (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                operation TEXT NOT NULL,
                status TEXT NOT NULL,
                profile TEXT,
                model TEXT,
                request_json TEXT NOT NULL DEFAULT '{}',
                response_json TEXT NOT NULL DEFAULT '{}',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                finished_at TEXT
            )""",
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_studio_events_created ON studio_events(created_at DESC)")
        conn.commit()


def _load_studio_config() -> None:
    global QUEUE_MAX_WORKERS, IMAGE_MAX_WORKERS, VIDEO_MAX_WORKERS, GENERATION_TIMEOUT_SECONDS, MEDIA_URL_TTL_SECONDS, ASSET_RETENTION_SECONDS, PUBLIC_BASE_URL, GENERATION_RATE_CAPACITY, GENERATION_RATE_REFILL_SECONDS
    cli_settings = get_settings()
    GENERATION_RATE_CAPACITY = cli_settings.generation_rate_capacity
    GENERATION_RATE_REFILL_SECONDS = cli_settings.generation_rate_refill_seconds
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        settings = {str(row[0]): str(row[1]) for row in conn.execute("SELECT key, value FROM studio_settings")}
    if settings.get("queue_workers"):
        QUEUE_MAX_WORKERS = max(1, min(int(settings["queue_workers"]), 32))
    if settings.get("image_workers"):
        IMAGE_MAX_WORKERS = max(1, min(int(settings["image_workers"]), 32))
    if settings.get("video_workers"):
        VIDEO_MAX_WORKERS = max(1, min(int(settings["video_workers"]), 32))
    if settings.get("generation_timeout_seconds"):
        GENERATION_TIMEOUT_SECONDS = max(60, min(int(settings["generation_timeout_seconds"]), 86_400))
    if settings.get("media_url_ttl_seconds"):
        MEDIA_URL_TTL_SECONDS = max(300, min(int(settings["media_url_ttl_seconds"]), 30 * 86400))
    if settings.get("asset_retention_seconds"):
        ASSET_RETENTION_SECONDS = max(3600, min(int(settings["asset_retention_seconds"]), 90 * 86400))
    if settings.get("generation_rate_capacity"):
        GENERATION_RATE_CAPACITY = max(1, min(int(settings["generation_rate_capacity"]), 1000))
    if settings.get("generation_rate_refill_seconds"):
        GENERATION_RATE_REFILL_SECONDS = max(0.1, min(float(settings["generation_rate_refill_seconds"]), 86400.0))
    if settings.get("public_base_url") is not None:
        PUBLIC_BASE_URL = settings["public_base_url"].rstrip("/")


def _save_studio_settings(values: dict[str, Any]) -> None:
    now = datetime.now(UTC).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            "INSERT INTO studio_settings(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            [(key, str(value), now) for key, value in values.items()],
        )
        conn.commit()


def _record_event(
    *,
    source: str,
    operation: str,
    status: str,
    profile: str | None = None,
    model: str | None = None,
    request: Any = None,
    response: Any = None,
    error: str = "",
    event_id: str | None = None,
    finished_at: str | None = None,
) -> str:
    event_id = event_id or str(uuid.uuid4())
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO studio_events(id, source, operation, status, profile, model, request_json, response_json, error, created_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET status=excluded.status, response_json=excluded.response_json, error=excluded.error, finished_at=excluded.finished_at",
            (
                event_id,
                source,
                operation,
                status,
                profile,
                model,
                json.dumps(request or {}, ensure_ascii=False, default=str),
                json.dumps(response or {}, ensure_ascii=False, default=str),
                error,
                datetime.now(UTC).isoformat(),
                finished_at,
            ),
        )
        conn.commit()
    return event_id


def _event_logs(profile: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    _init_db()
    query = "SELECT id, source, operation, status, profile, model, request_json, response_json, error, created_at, finished_at FROM studio_events"
    params: list[Any] = []
    if profile:
        query += " WHERE profile = ?"
        params.append(profile)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(max(1, min(limit, 10_000)))
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for row in rows:
        try:
            request = json.loads(row[6])
            response = json.loads(row[7])
        except json.JSONDecodeError:
            request, response = {}, {}
        result.append({
            "id": row[0], "source": row[1], "operation": row[2], "status": row[3],
            "profile": row[4], "model": row[5], "request": request, "response": response,
            "error": row[8] or None, "created_at": row[9], "finished_at": row[10],
            "kind": "image" if "image" in str(row[2]) else ("video" if "video" in str(row[2]) else "operation"),
        })
    return result


def _asset_record(asset_id: str) -> dict[str, Any] | None:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT asset_id, filename, local_path, bytes, sha256, created_at, last_accessed_at, expires_at FROM studio_assets WHERE asset_id = ?", (asset_id,)).fetchone()
    if row is None:
        return None
    return {"asset_id": row[0], "filename": row[1], "path": row[2], "bytes": row[3], "sha256": row[4], "created_at": row[5], "last_accessed_at": row[6], "expires_at": row[7]}


def _touch_asset(asset_id: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE studio_assets SET last_accessed_at = ? WHERE asset_id = ?",
            (datetime.now(UTC).isoformat(), asset_id),
        )
        conn.commit()


def _public_asset(record: dict[str, Any]) -> dict[str, Any]:
    public = dict(record)
    public.pop("path", None)
    public["preview_url"] = _media_url(record["path"])
    return public


def _save_asset_record(*, asset_id: str, filename: str, path: Path, size: int, sha256: str) -> dict[str, Any]:
    now = datetime.now(UTC)
    expires = now.timestamp() + ASSET_RETENTION_SECONDS
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO studio_assets(asset_id, filename, local_path, bytes, sha256, created_at, last_accessed_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(asset_id) DO UPDATE SET filename=excluded.filename, local_path=excluded.local_path, bytes=excluded.bytes, sha256=excluded.sha256, expires_at=excluded.expires_at",
            (asset_id, filename, str(path), size, sha256, now.isoformat(), None, datetime.fromtimestamp(expires, UTC).isoformat()),
        )
        conn.commit()
    return {"asset_id": asset_id, "filename": filename, "path": str(path), "bytes": size, "sha256": sha256, "expires_at": datetime.fromtimestamp(expires, UTC).isoformat(), "preview_url": _media_url(str(path))}


def _profile_setting(name: str) -> dict[str, Any]:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT remark, enabled, image_concurrency, video_concurrency FROM profile_settings WHERE profile_name = ?",
            (name,),
        ).fetchone()
    if row is None:
        return {"remark": "", "enabled": True, "image_concurrency": 1, "video_concurrency": 1}
    return {
        "remark": str(row[0] or ""),
        "enabled": bool(row[1]),
        "image_concurrency": max(1, min(int(row[2] or 1), 1)),
        "video_concurrency": max(1, min(int(row[3] or 1), 1)),
    }


def _profile_setting_names() -> set[str]:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT profile_name FROM profile_settings").fetchall()
    return {str(row[0]) for row in rows}


def _known_profile_names() -> set[str]:
    return {item.name for item in profile_store.list_profiles()} | _profile_setting_names()


def _profile_health(name: str) -> dict[str, Any]:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT auth_state, last_checked_at, last_success_at, last_failure_at, last_error, consecutive_failures "
            "FROM profile_health WHERE profile_name = ?",
            (name,),
        ).fetchone()
    if row is None:
        return {
            "auth_state": "unknown",
            "last_checked_at": None,
            "last_success_at": None,
            "last_failure_at": None,
            "last_error": "",
            "consecutive_failures": 0,
        }
    return {
        "auth_state": str(row[0] or "unknown"),
        "last_checked_at": row[1],
        "last_success_at": row[2],
        "last_failure_at": row[3],
        "last_error": str(row[4] or ""),
        "consecutive_failures": int(row[5] or 0),
    }


def _record_profile_health(name: str, result: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    status = str(result.get("status") or "error")
    error = result.get("error") if isinstance(result.get("error"), dict) else {}
    error_status = error.get("status")
    try:
        error_status = int(error_status) if error_status is not None else None
    except (TypeError, ValueError):
        error_status = None
    retryable = bool(error.get("retryable"))
    if status == "authenticated":
        state = "healthy"
        error_text = ""
    elif error_status == 503 or retryable:
        state = "unavailable"
        error_text = str(error.get("detail") or error.get("message") or "Flow 会话验证暂时不可用")
    else:
        state = "invalid"
        error_text = str(error.get("detail") or error.get("message") or result.get("detail") or "Flow 会话无效")
    previous = _profile_health(name)
    failures = 0 if state == "healthy" else previous["consecutive_failures"] + 1
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO profile_health(profile_name, auth_state, last_checked_at, last_success_at, last_failure_at, last_error, consecutive_failures) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(profile_name) DO UPDATE SET auth_state=excluded.auth_state, last_checked_at=excluded.last_checked_at, "
            "last_success_at=excluded.last_success_at, last_failure_at=excluded.last_failure_at, last_error=excluded.last_error, "
            "consecutive_failures=excluded.consecutive_failures",
            (
                name,
                state,
                now,
                now if state == "healthy" else previous["last_success_at"],
                now if state != "healthy" else previous["last_failure_at"],
                error_text,
                failures,
            ),
        )
        conn.commit()
    return _profile_health(name)


def _profile_is_pool_eligible(name: str) -> bool:
    """Keep cookie-bearing Profiles in the pool until a real task fails.

    The former implementation ran the legacy labs.google session probe and
    removed Profiles when that probe returned a false negative. The current
    Flow protocol is validated by the actual generation request instead.
    Health rows remain useful for diagnostics, but they are not a scheduler
    admission gate.
    """
    return True


def _pool_profiles() -> list[profile_store.ProfileMeta]:
    """Return enabled, cookie-bearing Profiles eligible for real-task checks."""
    return [
        item
        for item in profile_store.list_profiles()
        if _profile_setting(item.name)["enabled"] and item.cookies_present and _profile_is_pool_eligible(item.name)
    ]


def _pool_capacity(kind: Literal["image", "video"]) -> int:
    """Return the safe effective capacity of the eligible Profile pool.

    gflow-cli's persistent Chrome Profile lease is exclusive, so one Profile
    contributes one real slot per media kind. This is intentionally separate
    from the global Worker setting, which is only an upper bound.
    """
    setting_key = f"{kind}_concurrency"
    return sum(int(_profile_setting(item.name)[setting_key]) for item in _pool_profiles())


def _resolve_execution_profile(requested: str | None) -> str:
    """Resolve an explicit profile or choose the least-loaded pool member."""
    global POOL_CURSOR
    requested = requested or "auto"
    records = {item.name: item for item in profile_store.list_profiles()}
    if requested != "auto":
        item = records.get(requested)
        if item is None:
            raise RuntimeError(f"Profile 不存在：{requested}")
        setting = _profile_setting(requested)
        if not setting["enabled"]:
            raise RuntimeError(f"Profile 已禁用：{requested}")
        if not item.cookies_present:
            raise RuntimeError(f"Profile 尚未完成登录：{requested}")
        return requested

    candidates = _pool_profiles()
    if not candidates:
        raise RuntimeError("账号池没有可用账号，请先添加并登录至少一个启用的 Profile")
    loads = {
        item.name: sum(
            task.get("profile") == item.name and task.get("status") in {"queued", "running", "cancelling"}
            for task in TASKS.values()
        )
        for item in candidates
    }
    min_load = min(loads.values())
    eligible = [item for item in candidates if loads[item.name] == min_load]
    eligible.sort(key=lambda item: item.name)
    selected = eligible[POOL_CURSOR % len(eligible)]
    POOL_CURSOR = (POOL_CURSOR + 1) % max(1, len(eligible))
    return selected.name


def _execution_candidates(requested: str | None) -> list[str]:
    """Return ordered candidates for one request, best loaded first."""
    global POOL_CURSOR
    requested = requested or "auto"
    records = {item.name: item for item in profile_store.list_profiles()}
    if requested != "auto":
        item = records.get(requested)
        if item is None:
            raise RuntimeError(f"Profile 不存在：{requested}")
        setting = _profile_setting(requested)
        if not setting["enabled"]:
            raise RuntimeError(f"Profile 已禁用：{requested}")
        if not item.cookies_present:
            raise RuntimeError(f"Profile 尚未完成登录：{requested}")
        return [requested]
    candidates = _pool_profiles()
    loads = {
        item.name: sum(
            task.get("profile") == item.name and task.get("status") in {"queued", "running", "cancelling"}
            for task in TASKS.values()
        )
        for item in candidates
    }
    ordered = sorted(candidates, key=lambda item: (loads[item.name], item.name))
    if not ordered:
        return []
    min_load = loads[ordered[0].name]
    best = [item.name for item in ordered if loads[item.name] == min_load]
    rest = [item.name for item in ordered if loads[item.name] != min_load]
    start = POOL_CURSOR % len(best)
    POOL_CURSOR = (POOL_CURSOR + 1) % len(best)
    return best[start:] + best[:start] + rest


def _upsert_profile_setting(
    name: str,
    *,
    remark: str | None = None,
    enabled: bool | None = None,
    image_concurrency: int | None = None,
    video_concurrency: int | None = None,
) -> None:
    current = _profile_setting(name)
    # A persistent Chrome Profile is protected by an exclusive ProfileLease.
    # Keep the account-level cap explicit, but do not accept a value that the
    # browser runtime cannot safely honour.
    safe_image_concurrency = 1 if image_concurrency is None else max(1, min(int(image_concurrency), 1))
    safe_video_concurrency = 1 if video_concurrency is None else max(1, min(int(video_concurrency), 1))
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO profile_settings(profile_name, remark, enabled, image_concurrency, video_concurrency, updated_at) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(profile_name) DO UPDATE SET remark=excluded.remark, enabled=excluded.enabled, "
            "image_concurrency=excluded.image_concurrency, video_concurrency=excluded.video_concurrency, "
            "updated_at=excluded.updated_at",
            (
                name,
                current["remark"] if remark is None else remark,
                int(current["enabled"] if enabled is None else enabled),
                safe_image_concurrency if image_concurrency is not None else current["image_concurrency"],
                safe_video_concurrency if video_concurrency is not None else current["video_concurrency"],
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()


def _reset_profile_health(name: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM profile_health WHERE profile_name = ?", (name,))
        conn.commit()


def _profile_project_rows(name: str, *, enabled_only: bool = False) -> list[dict[str, Any]]:
    _init_db()
    query = (
        "SELECT project_id, title, enabled, is_default, use_count, last_used_at, created_at, updated_at "
        "FROM profile_projects WHERE profile_name = ? "
        + ("AND enabled = 1 " if enabled_only else "")
        + "ORDER BY is_default DESC, enabled DESC, COALESCE(last_used_at, ''), project_id"
    )
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(query, (name,)).fetchall()
    return [
        {
            "profile": name,
            "project_id": str(row[0]),
            "title": str(row[1] or ""),
            "enabled": bool(row[2]),
            "is_default": bool(row[3]),
            "use_count": int(row[4] or 0),
            "last_used_at": row[5],
            "created_at": row[6],
            "updated_at": row[7],
        }
        for row in rows
    ]


def _upsert_profile_project(
    profile: str,
    project_id: str,
    *,
    title: str = "",
    enabled: bool | None = None,
    is_default: bool | None = None,
) -> dict[str, Any]:
    project_id = _validate_project_id(project_id)
    now = datetime.now(UTC).isoformat()
    current = next((item for item in _profile_project_rows(profile) if item["project_id"] == project_id), None)
    next_enabled = current["enabled"] if current and enabled is None else (True if enabled is None else enabled)
    next_default = current["is_default"] if current and is_default is None else bool(is_default)
    if not next_enabled:
        next_default = False
    with sqlite3.connect(DB_PATH) as conn:
        if next_default:
            conn.execute("UPDATE profile_projects SET is_default = 0, updated_at = ? WHERE profile_name = ?", (now, profile))
        conn.execute(
            "INSERT INTO profile_projects(profile_name, project_id, title, enabled, is_default, use_count, last_used_at, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(profile_name, project_id) DO UPDATE SET title=excluded.title, enabled=excluded.enabled, "
            "is_default=excluded.is_default, updated_at=excluded.updated_at",
            (
                profile,
                project_id,
                title or (current["title"] if current else ""),
                int(next_enabled),
                int(next_default),
                current["use_count"] if current else 0,
                current["last_used_at"] if current else None,
                current["created_at"] if current else now,
                now,
            ),
        )
        conn.commit()
    return next(item for item in _profile_project_rows(profile) if item["project_id"] == project_id)


def _mark_project_used(profile: str, project_id: str) -> None:
    now = datetime.now(UTC).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "UPDATE profile_projects SET use_count = use_count + 1, last_used_at = ?, updated_at = ? "
            "WHERE profile_name = ? AND project_id = ?",
            (now, now, profile, project_id),
        )
        conn.commit()


def _delete_profile_project(profile: str, project_id: str) -> bool:
    project_id = _validate_project_id(project_id)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("DELETE FROM profile_projects WHERE profile_name = ? AND project_id = ?", (profile, project_id))
        conn.commit()
    return cursor.rowcount > 0


def _validate_project_id(project_id: str) -> str:
    if not PROJECT_RE.fullmatch(project_id):
        raise ValueError("project_id 只能包含字母、数字、下划线和短横线")
    return project_id


def _delete_profile_setting(name: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM profile_settings WHERE profile_name = ?", (name,))
        conn.commit()


def _persist_task(task: dict[str, Any]) -> None:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO studio_tasks(id, payload_json, task_json, updated_at, status, idempotency_key, request_hash, available_at, attempts, timeout_seconds) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET payload_json=excluded.payload_json, task_json=excluded.task_json, "
            "updated_at=excluded.updated_at, status=excluded.status, idempotency_key=excluded.idempotency_key, "
            "request_hash=excluded.request_hash, available_at=excluded.available_at, attempts=excluded.attempts, "
            "timeout_seconds=excluded.timeout_seconds",
            (
                task["id"],
                json.dumps(task.get("request", {}), ensure_ascii=False),
                json.dumps(task, ensure_ascii=False, default=str),
                datetime.now(UTC).isoformat(),
                str(task.get("status") or "queued"),
                task.get("idempotency_key"),
                task.get("request_hash"),
                task.get("available_at"),
                int(task.get("attempts", 0)),
                int(task.get("timeout_seconds", GENERATION_TIMEOUT_SECONDS)),
            ),
        )
        conn.commit()


def _set_task_progress(task: dict[str, Any], progress: int, phase: str) -> None:
    # A separate API process may set cancel_requested/cancelling while the
    # Worker is progressing. Merge those control-plane flags before writing a
    # progress snapshot so a harmless progress update cannot erase a cancel.
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT task_json FROM studio_tasks WHERE id = ?", (task.get("id"),)).fetchone()
    if row:
        try:
            persisted = json.loads(row[0])
            if persisted.get("cancel_requested"):
                task["cancel_requested"] = True
            if persisted.get("status") in {"cancelling", "cancelled"}:
                task["status"] = persisted["status"]
        except json.JSONDecodeError:
            pass
    task["progress"] = max(0, min(progress, 100))
    task["phase"] = phase
    task["progress_at"] = datetime.now(UTC).isoformat()
    _persist_task(task)


def _load_persisted_tasks(*, recover_running: bool = True) -> None:
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT task_json FROM studio_tasks ORDER BY updated_at DESC LIMIT 200").fetchall()
    for (raw,) in rows:
        try:
            task = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if recover_running and task.get("status") in {"running", "cancelling"}:
            task["status"] = "interrupted"
            task["error"] = "gflow studio 重启时任务尚未完成，提交结果未知；请确认 Flow 项目后再重试"
            task["finished_at"] = datetime.now(UTC).isoformat()
        TASKS[str(task["id"])] = task
        _persist_task(task)


def _load_task(task_id: str) -> dict[str, Any] | None:
    _init_db()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("SELECT task_json FROM studio_tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        return TASKS.get(task_id)
    try:
        task = json.loads(row[0])
    except json.JSONDecodeError:
        return None
    TASKS[task_id] = task
    return task


def _persisted_task_snapshots(limit: int = 10_000) -> list[dict[str, Any]]:
    """Read task history from SQLite, not only the bounded memory cache."""
    _init_db()
    limit = max(1, min(int(limit), 50_000))
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            "SELECT task_json FROM studio_tasks ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    snapshots: list[dict[str, Any]] = []
    for (raw,) in rows:
        try:
            task = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(task, dict) and task.get("id"):
            snapshots.append(task)
    return snapshots


def _task_snapshots(limit: int = 10_000) -> list[dict[str, Any]]:
    """Merge durable history with live in-process task objects."""
    snapshots = {str(item["id"]): item for item in _persisted_task_snapshots(limit)}
    for task_id, task in TASKS.items():
        snapshots[str(task_id)] = task
    return list(snapshots.values())


def _refresh_task_cache(*, all_queue_tasks: bool = False) -> None:
    """Refresh API-process task snapshots when a separate Worker is active."""
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        if all_queue_tasks:
            rows = conn.execute(
                "SELECT task_json FROM studio_tasks WHERE status IN ('queued', 'running', 'cancelling') "
                "ORDER BY available_at ASC, updated_at ASC"
            ).fetchall()
        else:
            rows = conn.execute("SELECT task_json FROM studio_tasks ORDER BY updated_at DESC LIMIT 200").fetchall()
    for (raw,) in rows:
        try:
            task = json.loads(raw)
        except json.JSONDecodeError:
            continue
        task_id = str(task.get("id"))
        # Keep the live object owned by the local asyncio task. Its progress
        # updates are persisted by _execute_generation; replacing it from a
        # DB snapshot here would make the dispatcher count correctly but could
        # detach the coroutine from the object returned by the API.
        handle = TASK_HANDLES.get(task_id)
        if handle is not None and not handle.done():
            continue
        TASKS[task_id] = task


def _acquire_worker_lease(worker_id: str | None = None) -> None:
    """Acquire the database-wide worker lease before dispatching tasks.

    A Chrome profile is not safely shareable between worker processes. The
    lease makes that constraint explicit and prevents accidental duplicate
    generation when systemd/Docker briefly starts two workers during a
    restart. A dead worker is recoverable after the heartbeat timeout.
    """
    worker_id = worker_id or QUEUE_WORKER_ID
    _init_db()
    now = time.time()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT worker_id, pid, heartbeat_at FROM studio_worker_lease WHERE id = 1").fetchone()
            if row and row[0] != worker_id and now - float(row[2]) < WORKER_LEASE_SECONDS and _process_alive(int(row[1])):
                raise RuntimeError(f"已有活动 Worker 持有队列租约：{row[0]}")
            conn.execute(
                "INSERT INTO studio_worker_lease(id, worker_id, pid, heartbeat_at) VALUES (1, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET worker_id=excluded.worker_id, pid=excluded.pid, heartbeat_at=excluded.heartbeat_at",
                (worker_id, os.getpid(), now),
            )
    finally:
        conn.close()


def _release_worker_lease(worker_id: str | None = None) -> None:
    worker_id = worker_id or QUEUE_WORKER_ID
    _init_db()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        with conn:
            conn.execute("DELETE FROM studio_worker_lease WHERE id = 1 AND worker_id = ?", (worker_id,))
    finally:
        conn.close()


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _worker_is_online() -> bool:
    _init_db()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("SELECT heartbeat_at FROM studio_worker_lease WHERE id = 1").fetchone()
    return bool(row and time.time() - float(row[0]) < WORKER_LEASE_SECONDS)


async def _worker_lease_heartbeat() -> None:
    global QUEUE_STOP
    while not QUEUE_STOP:
        await asyncio.sleep(max(5, WORKER_LEASE_SECONDS // 3))
        if QUEUE_STOP:
            break
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH, timeout=10)
            with conn:
                cursor = conn.execute(
                    "UPDATE studio_worker_lease SET heartbeat_at = ?, pid = ? WHERE id = 1 AND worker_id = ?",
                    (time.time(), os.getpid(), QUEUE_WORKER_ID),
                )
            if cursor.rowcount != 1:
                # Another worker took over a stale lease. Stop dispatching so
                # this process cannot create duplicate upstream jobs.
                QUEUE_STOP = True
                QUEUE_WAKE.set()
                return
        except sqlite3.Error:
            # A transient SQLite lock must not immediately kill a healthy
            # worker; the next heartbeat will retry.
            continue
        finally:
            if conn is not None:
                conn.close()


def _claim_task(task_id: str) -> dict[str, Any] | None:
    """Atomically claim a queued task and return its DB-authoritative value."""
    _init_db()
    now = datetime.now(UTC).isoformat()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT task_json, status, attempts FROM studio_tasks WHERE id = ?", (task_id,)).fetchone()
            if row is None or row[1] != "queued":
                return None
            try:
                task = json.loads(row[0])
            except json.JSONDecodeError:
                return None
            task["status"] = "running"
            task["attempts"] = int(row[2] or task.get("attempts", 0)) + 1
            task["claimed_by"] = QUEUE_WORKER_ID
            task["started_at"] = now
            conn.execute(
                "UPDATE studio_tasks SET payload_json = ?, task_json = ?, updated_at = ?, status = 'running', attempts = ?, timeout_seconds = ? WHERE id = ? AND status = 'queued'",
                (
                    json.dumps(task.get("request", {}), ensure_ascii=False),
                    json.dumps(task, ensure_ascii=False, default=str),
                    now,
                    task["attempts"],
                    int(task.get("timeout_seconds", GENERATION_TIMEOUT_SECONDS)),
                    task_id,
                ),
            )
    finally:
        conn.close()
    TASKS[task_id] = task
    return task


def _task_cancel_requested(task_id: str) -> bool:
    _init_db()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        row = conn.execute("SELECT task_json FROM studio_tasks WHERE id = ?", (task_id,)).fetchone()
    if row is None:
        return bool(TASKS.get(task_id, {}).get("cancel_requested"))
    try:
        return bool(json.loads(row[0]).get("cancel_requested"))
    except json.JSONDecodeError:
        return False


def _task_age_seconds(task: dict[str, Any], now: float | None = None) -> float:
    """Return the age of a task using its persisted timestamp.

    Monitoring must remain useful after an API/Worker restart, so this derives
    from the SQLite-backed task snapshot instead of an in-memory timer.
    """
    timestamp = task.get("started_at") or task.get("created_at")
    if not isinstance(timestamp, str):
        return 0.0
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        age = (datetime.now(UTC).timestamp() if now is None else now) - parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return max(0.0, age)


@app.on_event("startup")
async def load_state() -> None:
    global SHUTTING_DOWN, QUEUE_WAKE
    SHUTTING_DOWN = False
    # asyncio primitives are bound lazily to an event loop. TestClient and
    # some process supervisors can create a fresh loop across restarts; never
    # carry the previous loop's wake event into the new dispatcher.
    QUEUE_WAKE = asyncio.Event()
    if not TASK_HANDLES:
        PROFILE_LOCKS.clear()
    _load_studio_config()
    configure_rate_limiter(GENERATION_RATE_CAPACITY, GENERATION_RATE_REFILL_SECONDS)
    _load_persisted_tasks(recover_running=STUDIO_ROLE != "api")
    _cleanup_uploads()
    global QUEUE_STOP, QUEUE_DISPATCHER
    QUEUE_STOP = False
    if STUDIO_ROLE != "api":
        _acquire_worker_lease()
        global WORKER_LEASE_HEARTBEAT
        WORKER_LEASE_HEARTBEAT = asyncio.create_task(_worker_lease_heartbeat())
        QUEUE_DISPATCHER = asyncio.create_task(_queue_dispatcher())


@app.on_event("shutdown")
async def stop_state() -> None:
    global QUEUE_STOP, QUEUE_DISPATCHER, WORKER_LEASE_HEARTBEAT, SHUTTING_DOWN
    SHUTTING_DOWN = True
    QUEUE_STOP = True
    QUEUE_WAKE.set()
    if QUEUE_DISPATCHER is not None:
        QUEUE_DISPATCHER.cancel()
        await asyncio.gather(QUEUE_DISPATCHER, return_exceptions=True)
        QUEUE_DISPATCHER = None
    if WORKER_LEASE_HEARTBEAT is not None:
        WORKER_LEASE_HEARTBEAT.cancel()
        await asyncio.gather(WORKER_LEASE_HEARTBEAT, return_exceptions=True)
        WORKER_LEASE_HEARTBEAT = None
    if STUDIO_ROLE != "api":
        _release_worker_lease()
    for task in TASKS.values():
        if task.get("status") in {"running", "cancelling"}:
            task["status"] = "interrupted"
            task["finished_at"] = datetime.now(UTC).isoformat()
            task["error"] = "gflow studio 正在关闭，任务已中断；请确认 Flow 项目后重试"
            _persist_task(task)
    for handle in list(TASK_HANDLES.values()):
        handle.cancel()
    if TASK_HANDLES:
        await asyncio.gather(*TASK_HANDLES.values(), return_exceptions=True)


class GenerationRequest(BaseModel):
    kind: Literal["image", "video"] = "video"
    prompt: str = Field(min_length=1, max_length=20_000)
    profile: str | None = None
    project: str | None = None
    model: str | None = None
    requested_model: str | None = None
    model_label: str | None = None
    aspect: str = "9:16"
    duration: int | None = None
    count: int = Field(default=1, ge=1, le=4)
    mode: Literal["t2v", "i2v", "r2v"] = "t2v"
    initial_frame: str | None = None
    end_frame: str | None = None
    reference_images: list[str] = Field(default_factory=list, max_length=10)
    input_asset_ids: list[str] = Field(default_factory=list, max_length=10)
    wait: bool = False
    timeout_seconds: int = Field(default=GENERATION_TIMEOUT_SECONDS, ge=60, le=86_400)
    idempotency_key: str | None = Field(default=None, max_length=200)


class ProfileCreateRequest(BaseModel):
    name: str
    remark: str = Field(default="", max_length=200)
    # A persistent Profile has one exclusive browser lease, so the safe
    # account-level capacity is deliberately constrained to one.
    image_concurrency: int = Field(default=1, ge=1, le=1)
    video_concurrency: int = Field(default=1, ge=1, le=1)


class ProfileUpdateRequest(BaseModel):
    new_name: str | None = None
    remark: str | None = Field(default=None, max_length=200)
    enabled: bool | None = None
    image_concurrency: int | None = Field(default=None, ge=1, le=1)
    video_concurrency: int | None = Field(default=None, ge=1, le=1)


def _veo_model_has_no_duration_control(model: str | None) -> bool:
    if not model:
        return False
    normalized = model.strip().lower().replace("-", "_")
    return normalized in {
        "veo_lite",
        "veo_fast",
        "veo_quality",
        "veo_3_1_lite",
        "veo_3_1_fast",
        "veo_3_1_quality",
        "veo_lite_lp",
        "veo_3_1_lite_lower_priority",
    }


def _request_hash(request: GenerationRequest) -> str:
    payload = request.model_dump(exclude={"wait", "idempotency_key"})
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def _resolve_input_asset_ids(asset_ids: list[str]) -> list[str]:
    paths = []
    for asset_id in asset_ids:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", asset_id):
            raise HTTPException(status_code=400, detail=f"非法 asset_id：{asset_id}")
        record = _asset_record(asset_id)
        matches = [Path(record["path"])] if record else list(UPLOAD_DIR.glob(f"{asset_id}.*"))
        if not matches or not matches[0].is_file():
            raise HTTPException(status_code=404, detail=f"输入资产不存在：{asset_id}")
        _touch_asset(asset_id)
        paths.append(str(matches[0]))
    return paths


def _find_idempotent(key: str | None, request_hash: str) -> dict[str, Any] | None:
    if not key:
        return None
    cached = next((task for task in TASKS.values() if task.get("idempotency_key") == key), None)
    if cached is not None:
        if cached.get("request_hash") != request_hash:
            raise HTTPException(status_code=409, detail="幂等键已被其他请求使用")
        return cached
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT task_json, request_hash FROM studio_tasks WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
    if row is not None:
        if row[1] != request_hash:
            raise HTTPException(status_code=409, detail="幂等键已被其他请求使用")
        try:
            task = json.loads(row[0])
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=500, detail="幂等任务记录损坏") from exc
        TASKS[str(task["id"])] = task
        return task
    return None


async def _queue_dispatcher() -> None:
    """Durable single-process dispatcher.

    SQLite is the source of truth; TASKS is only an in-process cache. The
    service intentionally runs one dispatcher because a Chrome profile is a
    single active lease. Multiple profiles still run concurrently through the
    per-profile locks.
    """
    while not QUEUE_STOP:
        # The API and Worker intentionally have separate processes. Refresh
        # queued rows every cycle so tasks created after worker startup are
        # discovered without an in-memory queue or a broker dependency.
        _refresh_task_cache(all_queue_tasks=True)
        active_tasks = [task for task_id, task in TASKS.items() if task_id in TASK_HANDLES and not TASK_HANDLES[task_id].done()]
        active = len(active_tasks)
        active_images = sum(item.get("kind") == "image" for item in active_tasks)
        active_videos = sum(item.get("kind") == "video" for item in active_tasks)
        capacity = max(0, QUEUE_MAX_WORKERS - active)
        if capacity:
            queued = sorted(
                (
                    task
                    for task in TASKS.values()
                    if task.get("status") == "queued"
                    and (not task.get("available_at") or str(task.get("available_at")) <= datetime.now(UTC).isoformat())
                ),
                key=lambda task: (task.get("available_at") or task.get("created_at") or "", task.get("created_at") or ""),
            )
            selected = []
            for task in queued:
                kind_limit = IMAGE_MAX_WORKERS if task.get("kind") == "image" else VIDEO_MAX_WORKERS
                kind_active = active_images + sum(item.get("kind") == task.get("kind") for item in selected)
                if kind_active >= kind_limit:
                    continue
                selected.append(task)
                if len(selected) >= capacity:
                    break
            for candidate in selected:
                task_id = str(candidate["id"])
                if task_id in TASK_HANDLES:
                    continue
                task = _claim_task(task_id)
                if task is None:
                    continue
                try:
                    generation_request = GenerationRequest.model_validate(task.get("request") or {})
                except Exception as exc:
                    task["status"] = "failed"
                    task["error"] = f"持久化任务参数损坏：{type(exc).__name__}"
                    task["finished_at"] = datetime.now(UTC).isoformat()
                    _persist_task(task)
                    continue
                TASK_HANDLES[task_id] = asyncio.create_task(_execute_generation(task_id, generation_request))
        try:
            await asyncio.wait_for(QUEUE_WAKE.wait(), timeout=1.0)
            QUEUE_WAKE.clear()
        except asyncio.TimeoutError:
            pass


def _validate_profile(name: str) -> str:
    if not PROFILE_RE.fullmatch(name):
        raise HTTPException(status_code=400, detail="profile 只能包含字母、数字、下划线和短横线")
    return name


def _profile_payload(meta: profile_store.ProfileMeta) -> dict[str, Any]:
    coarse = profile_status(meta.name)
    setting = _profile_setting(meta.name)
    health = _profile_health(meta.name)
    profile_tasks = [
        task
        for task in TASKS.values()
        if task.get("profile") == meta.name
        and task.get("status") in {"queued", "running", "cancelling"}
    ]
    image_inflight = sum(
        task.get("kind") == "image" and task.get("status") in {"running", "cancelling"}
        for task in profile_tasks
    )
    video_inflight = sum(
        task.get("kind") == "video" and task.get("status") in {"running", "cancelling"}
        for task in profile_tasks
    )
    queued = sum(task.get("status") == "queued" for task in profile_tasks)
    return {
        "name": meta.name,
        "google_account": meta.google_account,
        "cookies_present": bool(coarse.get("cookies_present")),
        "is_default": meta.is_default,
        "last_used_at": meta.last_used_at.isoformat() if meta.last_used_at else None,
        "remark": setting["remark"],
        "enabled": setting["enabled"],
        # One persistent Chrome Profile has one exclusive browser lease.
        "image_concurrency": setting["image_concurrency"],
        "video_concurrency": setting["video_concurrency"],
        "effective_image_concurrency": 1,
        "effective_video_concurrency": 1,
        "image_inflight": image_inflight,
        "video_inflight": video_inflight,
        "queued_tasks": queued,
        "auth_state": health["auth_state"],
        "auth_checked_at": health["last_checked_at"],
        "auth_error": health["last_error"],
        "auth_failure_count": health["consecutive_failures"],
    }


def _error_detail(result: Any) -> str:
    if not isinstance(result, dict):
        return str(result)
    error = result.get("error")
    if isinstance(error, dict):
        return str(error.get("detail") or error.get("message") or error.get("title") or error)
    return str(result.get("detail") or result.get("message") or error or result)


def _is_auth_failure(result: Any) -> bool:
    error = result.get("error") if isinstance(result, dict) else None
    status = error.get("status") if isinstance(error, dict) else None
    text = _error_detail(result).lower()
    markers = ("invalid session", "unauthorized", "not authenticated", "login required", "session expired", "credits validation failed")
    return status in {401, 403} or any(marker in text for marker in markers)


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    """Remove local filesystem paths while keeping browser-usable previews."""
    public = dict(task)
    result = public.get("result")
    if isinstance(result, dict):
        result = dict(result)
        files = result.get("files")
        if isinstance(files, list):
            public_files = []
            for item in files:
                path = item.get("path") or item.get("local_path") or item.get("uri") if isinstance(item, dict) else item
                if not path:
                    continue
                path_text = str(path)
                name = Path(urlparse(path_text).path).name or "result"
                public_files.append({"name": name, "preview_url": _media_url(path_text)})
            result["files"] = public_files
        public["result"] = result
    public.pop("request", None)
    return public


def _resolve_media_path(path: str) -> Path:
    candidate = Path(path).expanduser().resolve()
    roots = [UPLOAD_DIR.resolve(), get_settings().output_dir.expanduser().resolve()]
    if not any(candidate.is_relative_to(root) for root in roots) or not candidate.is_file():
        raise HTTPException(status_code=404, detail="资产不存在或不在允许的结果目录")
    return candidate


def _configured_cloud_path(path: str) -> bool:
    """Return whether *path* is inside the configured gflow cloud prefix."""
    if not path.startswith(("s3://", "gs://")):
        return False
    storage_uri = get_settings().storage_uri
    if not storage_uri or not storage_uri.startswith(("s3://", "gs://")):
        return False
    prefix = storage_uri.rstrip("/") + "/"
    return path.startswith(prefix)


async def _cloud_media_stream(path: str):
    """Yield a configured S3/GCS object without loading a video into RAM."""
    try:
        from universal_pathlib import UPath
    except ImportError as exc:
        raise HTTPException(status_code=503, detail="云端结果存储未安装 universal_pathlib 依赖") from exc
    handle = await asyncio.to_thread(UPath(path).open, "rb")
    try:
        while True:
            chunk = await asyncio.to_thread(handle.read, 1024 * 1024)
            if not chunk:
                break
            yield chunk
    finally:
        await asyncio.to_thread(handle.close)


def _materialize_result_files(result: Any, *, profile: str, kind: str) -> dict[str, Any]:
    """Normalize and verify result files before a task is reported succeeded."""
    normalized = dict(result) if isinstance(result, dict) else {"raw": result}
    files = normalized.get("files")
    paths: list[str] = []
    if isinstance(files, list):
        for item in files:
            value = item.get("path") or item.get("local_path") or item.get("uri") if isinstance(item, dict) else item
            if not value:
                continue
            path_text = str(value)
            if path_text.startswith(("s3://", "gs://")):
                if _configured_cloud_path(path_text):
                    paths.append(path_text)
                continue
            candidate = Path(path_text).expanduser()
            if candidate.is_file():
                paths.append(str(candidate))
    # A completed gflow task can have a catalog row but an empty `files` field
    # after a daemon/API restart. Recover a verified local file from the shared
    # gflow catalog before declaring the control-plane task successful.
    if not paths:
        media_id = normalized.get("flow_media_id") or normalized.get("media_id")
        if media_id:
            settings = get_settings()
            rows = list_images if kind == "image" else list_videos
            try:
                catalog_rows = rows(db_path=settings.resolved_db_path(), profile=profile, limit=200, offset=0)
                for row in catalog_rows:
                    if str(getattr(row, "media_id", "")) != str(media_id):
                        continue
                    candidate = getattr(row, "local_path", None)
                    if candidate and Path(str(candidate)).is_file():
                        paths.append(str(candidate))
                        break
            except Exception:
                # Catalog recovery is best-effort; the task will still fail
                # closed below if no materialized file can be proven.
                pass
    normalized["files"] = paths
    return normalized


def _media_signature(path: str, expiry: int) -> str:
    raw = f"{path}\n{expiry}".encode("utf-8")
    return hmac.new(MEDIA_SIGNING_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()


def _media_token(path: str) -> str:
    return base64.urlsafe_b64encode(path.encode("utf-8")).decode("ascii").rstrip("=")


def _path_from_media_token(token: str) -> str:
    try:
        padding = "=" * (-len(token) % 4)
        return base64.urlsafe_b64decode((token + padding).encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=403, detail="结果链接无效") from exc


def _media_url(path: str) -> str:
    expiry = int(time.time()) + MEDIA_URL_TTL_SECONDS
    query = urlencode({"token": _media_token(path), "exp": expiry, "sig": _media_signature(path, expiry)})
    return f"{PUBLIC_BASE_URL}/api/media/signed?{query}" if PUBLIC_BASE_URL else f"/api/media/signed?{query}"


def _profile_lock(name: str) -> asyncio.Lock:
    return PROFILE_LOCKS.setdefault(name, asyncio.Lock())


def _public_mcp_value(value: Any) -> Any:
    """Replace materialized result paths in an MCP payload with signed URLs.

    gflow-cli deliberately returns local paths because it is also a terminal
    client. The studio MCP boundary is different: a remote caller cannot read
    the worker filesystem, so only paths that pass the same media-root guard
    are converted. Arbitrary strings, missing files and unrelated upstream
    metadata are left untouched.
    """
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"path", "local_path"} and isinstance(item, str):
                try:
                    if item.startswith(("s3://", "gs://")):
                        if not _configured_cloud_path(item):
                            raise ValueError("cloud path is outside the configured prefix")
                    else:
                        _resolve_media_path(item)
                    result["url"] = _media_url(item)
                    continue
                except (HTTPException, ValueError, OSError):
                    pass
            result[key] = _public_mcp_value(item)
        return result
    if isinstance(value, list):
        return [_public_mcp_value(item) for item in value]
    if isinstance(value, str):
        try:
            if value.startswith(("s3://", "gs://")) and _configured_cloud_path(value):
                return _media_url(value)
            candidate = Path(value).expanduser()
            if candidate.is_file():
                return _media_url(value)
        except (HTTPException, ValueError, OSError):
            pass
    return value


def _rewrite_mcp_payload(body: bytes, content_type: str = "") -> bytes:
    """Rewrite JSON or SSE MCP messages without changing the JSON-RPC shape."""
    if not body or "json" not in content_type.lower() and "event-stream" not in content_type.lower():
        return body
    if "event-stream" in content_type.lower():
        chunks = body.splitlines(keepends=True)
        rewritten: list[bytes] = []
        for line in chunks:
            if line.startswith(b"data:"):
                prefix, raw = line.split(b":", 1)
                payload = raw.lstrip()
                try:
                    parsed = json.loads(payload)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    rewritten.append(line)
                else:
                    encoded = json.dumps(_public_mcp_value(parsed), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    newline = b"\n" if line.endswith(b"\n") else b""
                    rewritten.append(prefix + b": " + encoded + newline)
            else:
                rewritten.append(line)
        return b"".join(rewritten)
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return body
    return json.dumps(_public_mcp_value(parsed), ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@app.get("/")
async def index() -> FileResponse:
    entry = DIST_DIR / "index.html"
    if not entry.is_file():
        raise HTTPException(
            status_code=503,
            detail="React 前端构建产物不存在，请先运行 npm install && npm run build",
        )
    return FileResponse(entry)


def _check_gateway_auth(request: Request) -> None:
    if not AUTH_REQUIRED and _is_loopback(request):
        return
    if not AUTH_REQUIRED or not (_valid_request_auth(request) or _valid_session(request.cookies.get("gflow_session"))):
        raise HTTPException(status_code=401, detail="需要 gflow studio API Key 或管理员会话")


@app.api_route("/mcp", methods=["GET", "POST", "DELETE"])
async def mcp_gateway(request: Request) -> Response:
    """Forward the standard MCP Streamable HTTP endpoint to gflow serve.

    The browser UI and external MCP clients can use one public origin while
    the actual gflow MCP server stays loopback-only. The proxy preserves MCP
    session headers and never exposes the internal upstream URL.
    """
    _check_gateway_auth(request)
    import httpx

    body = await request.body()
    if len(body) > 16 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="MCP 请求体不能超过 16MB，请先上传素材再传 asset_id")
    headers = {key: value for key, value in request.headers.items() if key.lower() not in {"host", "content-length"}}
    if MCP_UPSTREAM_AUTH:
        headers["authorization"] = f"Bearer {MCP_UPSTREAM_AUTH}"
    event_id = _record_event(source="mcp", operation="mcp.request", status="running", request={"method": request.method, "path": request.url.path, "bytes": len(body)})
    client = httpx.AsyncClient(timeout=None, follow_redirects=False)
    try:
        upstream_request = client.build_request(request.method, MCP_UPSTREAM_URL, params=request.query_params, headers=headers, content=body)
        upstream = await client.send(upstream_request, stream=True)
    except httpx.HTTPError as exc:
        await client.aclose()
        _record_event(source="mcp", operation="mcp.request", status="failed", request={"method": request.method, "path": request.url.path, "bytes": len(body)}, error=str(exc), event_id=event_id, finished_at=datetime.now(UTC).isoformat())
        raise HTTPException(status_code=502, detail=f"gflow MCP 上游不可用：{type(exc).__name__}") from exc
    response_headers = {
        key: value for key, value in upstream.headers.items()
        if key.lower() in {"content-type", "mcp-session-id", "mcp-protocol-version", "cache-control", "last-event-id", "allow"}
    }
    async def close_upstream() -> None:
        await upstream.aclose()
        await client.aclose()

    _record_event(source="mcp", operation="mcp.request", status="succeeded" if upstream.status_code < 400 else "failed", response={"status_code": upstream.status_code}, event_id=event_id, finished_at=datetime.now(UTC).isoformat())
    content_type = upstream.headers.get("content-type", "")
    if request.method == "POST" and "event-stream" not in content_type.lower():
        response_body = _rewrite_mcp_payload(await upstream.aread(), content_type)
        await close_upstream()
        response_headers.pop("content-length", None)
        return Response(response_body, status_code=upstream.status_code, headers=response_headers)

    async def relay_stream():
        buffer = b""
        try:
            async for chunk in upstream.aiter_raw():
                buffer += chunk
                if "event-stream" not in content_type.lower():
                    yield _rewrite_mcp_payload(buffer, content_type)
                    buffer = b""
                    continue
                while b"\n\n" in buffer:
                    event, buffer = buffer.split(b"\n\n", 1)
                    yield _rewrite_mcp_payload(event + b"\n\n", content_type)
            if buffer:
                yield _rewrite_mcp_payload(buffer, content_type)
        finally:
            await close_upstream()

    return StreamingResponse(relay_stream(), status_code=upstream.status_code, headers=response_headers)


def _openai_models_payload() -> dict[str, Any]:
    catalog = build_catalog()
    data = []
    for section in (catalog["image"], catalog["video"]):
        for item in section["models"]:
            aliases = item.get("aliases") or [item["name"]]
            data.append({"id": aliases[0], "object": "model", "created": 0, "owned_by": "gflow-cli", "aliases": aliases})
    return {"object": "list", "data": data}


@app.get("/v1/models")
async def openai_models(request: Request) -> dict[str, Any]:
    _check_gateway_auth(request)
    return _openai_models_payload()


class OpenAIGenerationRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=20_000)
    model: str | None = None
    n: int = Field(default=1, ge=1, le=4)
    size: str = "1024x1024"
    response_format: str = "url"
    input_images: list[str] = Field(default_factory=list, max_length=10)
    input_asset_ids: list[str] = Field(default_factory=list, max_length=10)
    profile: str | None = None
    project: str | None = None
    duration: int | None = None
    mode: Literal["t2v", "i2v", "r2v"] = "t2v"
    idempotency_key: str | None = None


def _aspect_from_size(size: str) -> str:
    if size in {"1024x1792", "1152x2048", "768x1344"}:
        return "9:16"
    if size in {"1792x1024", "2048x1152", "1344x768"}:
        return "16:9"
    return "1:1"


def _openai_images_response(task: dict[str, Any]) -> dict[str, Any]:
    files = (task.get("result") or {}).get("files", []) if isinstance(task.get("result"), dict) else []
    return {"created": int(time.time()), "data": [{"url": item.get("preview_url"), "revised_prompt": task.get("prompt", "")} for item in files if isinstance(item, dict)]}


def _openai_task_status(status: str) -> str:
    return {
        "queued": "queued",
        "running": "in_progress",
        "cancelling": "cancelling",
        "succeeded": "completed",
        "failed": "failed",
        "timed_out": "failed",
        "cancelled": "cancelled",
        "interrupted": "incomplete",
        "indeterminate": "incomplete",
    }.get(status, status)


def _absolute_media_url(value: str | None, request: Request | None = None) -> str | None:
    if not value:
        return value
    if request is not None and value.startswith("/"):
        return f"{str(request.base_url).rstrip('/')}{value}"
    return value


def _openai_video_response(task: dict[str, Any], request: Request | None = None) -> dict[str, Any]:
    public = _public_task(task)
    result = public.get("result") if isinstance(public.get("result"), dict) else {}
    files = result.get("files", []) if isinstance(result, dict) else []
    data = [
        {"url": _absolute_media_url(item.get("preview_url"), request), "mime_type": "video/mp4"}
        for item in files
        if isinstance(item, dict) and item.get("preview_url")
    ]
    video_url = data[0]["url"] if data else None
    response = {
        "id": task["id"],
        "object": "video.generation",
        "status": _openai_task_status(str(task.get("status", "queued"))),
        "progress": int(task.get("progress", 0)),
        "created": int(datetime.fromisoformat(str(task["created_at"]).replace("Z", "+00:00")).timestamp())
        if task.get("created_at")
        else int(time.time()),
        "completed": task.get("finished_at"),
        "model": task.get("request", {}).get("model") if isinstance(task.get("request"), dict) else None,
        "data": data,
        "error": task.get("error") if task.get("status") not in {"succeeded", "queued", "running", "cancelling"} else None,
        "task": public,
    }
    if video_url:
        # The Canvas OpenAI adapter checks these fields before falling back to
        # GET /content. Keep both names for OpenAI-compatible clients.
        response["url"] = video_url
        response["video_url"] = video_url
        response["content"] = {"url": video_url, "video_url": video_url}
    return response


async def _store_openai_video_image(upload: UploadFile) -> str:
    stored = await upload_asset(upload)
    record = _asset_record(str(stored["asset_id"]))
    if record is None:
        raise HTTPException(status_code=502, detail="参考图已上传但无法读取资产记录")
    return str(record["path"])


@app.post("/v1/images/generations")
async def openai_images(payload: OpenAIGenerationRequest, request: Request, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")) -> dict[str, Any]:
    _check_gateway_auth(request)
    if payload.response_format != "url":
        raise HTTPException(status_code=400, detail="当前兼容接口只支持 response_format=url")
    if any(item.startswith("data:") for item in payload.input_images):
        raise HTTPException(status_code=413, detail="请先通过 /api/assets 上传图片，再在生成请求中传 input_asset_ids；不接受大段 Base64")
    generation = GenerationRequest(
        kind="image", prompt=payload.prompt, model=payload.model, aspect=_aspect_from_size(payload.size), count=payload.n,
        reference_images=payload.input_images, input_asset_ids=payload.input_asset_ids, profile=payload.profile, project=payload.project,
        idempotency_key=idempotency_key or payload.idempotency_key, wait=True,
    )
    task = await create_generation(generation)
    if task.get("status") != "succeeded":
        raise HTTPException(status_code=502, detail=task.get("error") or "图片生成失败")
    return _openai_images_response(task)


@app.post("/v1/images/edits")
async def openai_image_edits(
    request: Request,
    prompt: str = Form(..., min_length=1, max_length=20_000),
    model: str | None = Form(default=None),
    n: int = Form(default=1),
    size: str = Form(default="1024x1024"),
    response_format: str = Form(default="url"),
    profile: str | None = Form(default=None),
    project: str | None = Form(default=None),
    image: list[UploadFile] = File(default=[]),
    file: list[UploadFile] = File(default=[]),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    """OpenAI multipart image-edit compatibility endpoint.

    OpenAI clients send one or more repeated ``image`` fields (some clients
    use ``file`` instead). Each file is persisted through the normal asset
    store first; the generation task only contains asset IDs, so the large
    multipart body never crosses the generation/MCP queue boundary.
    """
    _check_gateway_auth(request)
    if response_format != "url":
        raise HTTPException(status_code=400, detail="当前兼容接口只支持 response_format=url")
    if not 1 <= n <= 4:
        raise HTTPException(status_code=400, detail="n 必须在 1 到 4 之间")
    uploads = [item for item in [*image, *file] if item is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="至少需要一个 image 或 file 图片字段")

    asset_ids: list[str] = []
    for upload in uploads[:10]:
        stored = await upload_asset(upload)
        asset_id = stored.get("asset_id")
        if not asset_id:
            raise HTTPException(status_code=502, detail="图片已上传但未获得 asset_id")
        asset_ids.append(str(asset_id))

    generation = GenerationRequest(
        kind="image",
        prompt=prompt,
        model=model,
        aspect=_aspect_from_size(size),
        count=n,
        input_asset_ids=asset_ids,
        profile=profile,
        project=project,
        idempotency_key=idempotency_key,
        wait=True,
    )
    task = await create_generation(generation)
    if task.get("status") != "succeeded":
        raise HTTPException(status_code=502, detail=task.get("error") or "图片编辑失败")
    return _openai_images_response(task)


@app.post("/v1/videos", status_code=202)
async def openai_standard_videos(
    request: Request,
    prompt: str = Form(..., min_length=1, max_length=20_000),
    model: str | None = Form(default=None),
    seconds: int | None = Form(default=None),
    size: str = Form(default="9:16"),
    mode: str = Form(default="frames"),
    profile: str | None = Form(default=None),
    project: str | None = Form(default=None),
    first_frame: UploadFile | None = File(default=None),
    last_frame: UploadFile | None = File(default=None),
    images: list[UploadFile] | None = File(default=None, alias="image[]"),
    single_image: UploadFile | None = File(default=None, alias="image"),
    videos: list[UploadFile] | None = File(default=None, alias="video[]"),
    audios: list[UploadFile] | None = File(default=None, alias="audio[]"),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    """OpenAI-compatible multipart video creation endpoint.

    Infinite Canvas follows the standard ``POST /v1/videos`` contract and
    sends reference frames as multipart files. The studio's older
    ``/v1/videos/generations`` endpoint is JSON-only, so keeping this adapter
    here avoids forcing clients to know the internal studio route.
    """
    _check_gateway_auth(request)
    if videos or audios:
        raise HTTPException(status_code=400, detail="当前 gflow 视频接口只支持图片参考帧")

    uploaded = list(images or [])
    if single_image is not None:
        uploaded.append(single_image)
    image_paths: list[str] = []
    for upload in uploaded:
        image_paths.append(await _store_openai_video_image(upload))

    first_path = await _store_openai_video_image(first_frame) if first_frame is not None else None
    last_path = await _store_openai_video_image(last_frame) if last_frame is not None else None
    if first_path is None and image_paths and mode != "reference":
        first_path = image_paths[0]
    if last_path is None and len(image_paths) > 1 and mode != "reference":
        last_path = image_paths[1]

    normalized_mode = str(mode or "frames").lower()
    if normalized_mode == "reference" or len(image_paths) > 2:
        generation_mode: Literal["t2v", "i2v", "r2v"] = "r2v"
        reference_images = image_paths
        initial_frame = None
        end_frame = None
    elif first_path is not None:
        generation_mode = "i2v"
        reference_images = []
        initial_frame = first_path
        end_frame = last_path
    else:
        generation_mode = "t2v"
        reference_images = []
        initial_frame = None
        end_frame = None

    task = await create_generation(
        GenerationRequest(
            kind="video",
            prompt=prompt,
            model=model,
            aspect=_aspect_from_size(size),
            duration=seconds,
            mode=generation_mode,
            initial_frame=initial_frame,
            end_frame=end_frame,
            reference_images=reference_images,
            profile=profile,
            project=project or None,
            wait=False,
            idempotency_key=idempotency_key,
        )
    )
    return _openai_video_response(task, request)


@app.post("/v1/videos/generations")
async def openai_videos(payload: OpenAIGenerationRequest, request: Request, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")) -> dict[str, Any]:
    _check_gateway_auth(request)
    if any(item.startswith("data:") for item in payload.input_images):
        raise HTTPException(status_code=413, detail="请先通过 /api/assets 上传图片，再在生成请求中传 input_asset_ids；不接受大段 Base64")
    generation = GenerationRequest(
        kind="video", prompt=payload.prompt, model=payload.model, aspect=_aspect_from_size(payload.size), count=payload.n,
        mode=payload.mode, duration=payload.duration, reference_images=payload.input_images, input_asset_ids=payload.input_asset_ids,
        profile=payload.profile, project=payload.project, idempotency_key=idempotency_key or payload.idempotency_key, wait=False,
    )
    task = await create_generation(generation)
    return _openai_video_response(task, request)


async def _openai_generation_lookup(task_id: str, request: Request) -> dict[str, Any]:
    _check_gateway_auth(request)
    task = _load_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="生成任务不存在")
    return _openai_video_response(task, request)


@app.get("/v1/videos/{task_id}/content")
async def openai_video_content(task_id: str, request: Request) -> Response:
    """Serve a completed video for clients that use the OpenAI content path."""
    _check_gateway_auth(request)
    task = _load_task(task_id)
    if task is None or task.get("kind") != "video":
        raise HTTPException(status_code=404, detail="视频任务不存在")
    if task.get("status") != "succeeded":
        raise HTTPException(status_code=404, detail="视频结果尚未就绪")
    result = task.get("result") if isinstance(task.get("result"), dict) else {}
    files = result.get("files", []) if isinstance(result, dict) else []
    path_value = None
    for item in files if isinstance(files, list) else []:
        value = item.get("path") or item.get("local_path") or item.get("uri") if isinstance(item, dict) else item
        if value:
            path_value = str(value)
            break
    if not path_value:
        raise HTTPException(status_code=404, detail="视频结果文件不存在")
    if path_value.startswith(("s3://", "gs://")):
        if not _configured_cloud_path(path_value):
            raise HTTPException(status_code=404, detail="视频结果不在允许的云端存储目录")
        return StreamingResponse(_cloud_media_stream(path_value), media_type="video/mp4")
    return FileResponse(_resolve_media_path(path_value), media_type="video/mp4", filename=f"{task_id}.mp4")


@app.get("/v1/videos/generations/{task_id}")
@app.get("/v1/videos/{task_id}")
async def openai_video_status(task_id: str, request: Request) -> dict[str, Any]:
    return await _openai_generation_lookup(task_id, request)


@app.post("/v1/videos/generations/{task_id}/cancel")
async def openai_video_cancel(task_id: str, request: Request) -> dict[str, Any]:
    _check_gateway_auth(request)
    return _openai_video_response(await cancel_generation(task_id), request)


@app.post("/v1/videos/generations/{task_id}/retry")
async def openai_video_retry(task_id: str, request: Request) -> dict[str, Any]:
    _check_gateway_auth(request)
    return _openai_video_response(await retry_generation(task_id), request)


@app.get("/v1/generations/{task_id}")
async def openai_generation_status(task_id: str, request: Request) -> dict[str, Any]:
    return await _openai_generation_lookup(task_id, request)


def _responses_input(payload: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    """Extract a prompt and non-inline image references from Responses input.

    The control plane intentionally accepts references as HTTPS URLs or
    previously uploaded ``asset:...`` IDs. Inline data URLs would recreate the
    large-request problem this adapter is designed to avoid.
    """
    text_parts: list[str] = []
    urls: list[str] = []
    asset_ids: list[str] = []

    instructions = payload.get("instructions")
    if isinstance(instructions, str) and instructions.strip():
        text_parts.append(instructions.strip())

    source = payload.get("input")
    if isinstance(source, str):
        text_parts.append(source)
    elif isinstance(source, list):
        for item in source:
            if isinstance(item, str):
                text_parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            content = item.get("content", item.get("input"))
            parts = content if isinstance(content, list) else [content]
            for part in parts:
                if isinstance(part, str):
                    text_parts.append(part)
                    continue
                if not isinstance(part, dict):
                    continue
                part_type = str(part.get("type") or "")
                text = part.get("text") or part.get("input_text")
                if isinstance(text, str):
                    text_parts.append(text)
                image = part.get("image_url") or part.get("input_image")
                if isinstance(image, dict):
                    image = image.get("url")
                if isinstance(image, str):
                    if image.startswith("data:"):
                        raise HTTPException(status_code=413, detail="Responses 兼容入口不接受 inline Base64，请先通过 /api/assets 上传图片")
                    (asset_ids if image.startswith("asset:") else urls).append(image.removeprefix("asset:"))
                elif part_type in {"input_image", "image_url"} and image is not None:
                    raise HTTPException(status_code=400, detail="Responses 图片引用格式无效")

    return "\n".join(item.strip() for item in text_parts if item.strip()).strip(), urls, asset_ids


def _openai_response_payload(task: dict[str, Any], *, model: str) -> dict[str, Any]:
    public = _public_task(task)
    status = _openai_task_status(str(task.get("status", "queued")))
    body = json.dumps(public, ensure_ascii=False, separators=(",", ":"))
    content = [{"type": "output_text", "text": body, "annotations": []}]
    return {
        "id": f"resp_{task['id'].replace('-', '')}",
        "object": "response",
        "created_at": int(time.time()),
        "status": status,
        "model": model,
        "output": [{
            "id": f"msg_{task['id'].replace('-', '')}",
            "type": "message",
            "status": "completed" if status == "completed" else status,
            "role": "assistant",
            "content": content,
        }],
        "output_text": body,
        "error": {"type": "generation_error", "message": task.get("error")} if task.get("error") else None,
        "gflow_task_id": task["id"],
    }


@app.post("/v1/responses")
async def openai_responses(payload: dict[str, Any], request: Request, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")) -> dict[str, Any]:
    """OpenAI Responses-shaped facade for image/video generation clients."""
    _check_gateway_auth(request)
    prompt, urls, asset_ids = _responses_input(payload)
    if not prompt:
        raise HTTPException(status_code=400, detail="Responses input 中没有可生成的文本提示词")
    model = str(payload.get("model") or "nano-pro")
    is_video = any(token in model.lower() for token in ("veo", "omni", "video"))
    extension = payload.get("gflow") if isinstance(payload.get("gflow"), dict) else {}
    generation = GenerationRequest(
        kind="video" if is_video else "image",
        prompt=prompt,
        model=model,
        aspect=str(extension.get("aspect") or "9:16"),
        duration=extension.get("duration"),
        mode=str(extension.get("mode") or "t2v"),
        reference_images=urls,
        input_asset_ids=asset_ids,
        profile=extension.get("profile"),
        project=extension.get("project"),
        count=int(extension.get("count") or 1),
        idempotency_key=idempotency_key or payload.get("idempotency_key"),
        wait=bool(payload.get("wait", not is_video)),
    )
    task = await create_generation(generation)
    return _openai_response_payload(task, model=model)


@app.post("/v1/chat/completions")
async def openai_chat(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    """Small OpenAI-compatible media adapter for clients that only expose chat."""
    _check_gateway_auth(request)
    messages = payload.get("messages") or []
    prompt_parts = []
    for message in messages:
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str):
            prompt_parts.append(content)
        elif isinstance(content, list):
            prompt_parts.extend(item.get("text", "") for item in content if isinstance(item, dict) and item.get("type") == "text")
    prompt = "\n".join(item for item in prompt_parts if item).strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="messages 中没有可生成的文本提示词")
    model = str(payload.get("model") or "nano-pro")
    kind = "video" if any(token in model.lower() for token in ("veo", "omni", "video")) else "image"
    generation = GenerationRequest(kind=kind, prompt=prompt, model=model, wait=True)
    task = await create_generation(generation)
    return {"id": f"chatcmpl-{task['id']}", "object": "chat.completion", "created": int(time.time()), "model": model, "choices": [{"index": 0, "message": {"role": "assistant", "content": json.dumps(_public_task(task), ensure_ascii=False)}, "finish_reason": "stop"}]}


def _gemini_prompt(payload: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    texts: list[str] = []
    urls: list[str] = []
    asset_ids: list[str] = []
    for content in payload.get("contents") or []:
        for part in (content.get("parts") or []) if isinstance(content, dict) else []:
            if not isinstance(part, dict):
                continue
            if isinstance(part.get("text"), str):
                texts.append(part["text"])
            file_data = part.get("fileData")
            if isinstance(file_data, dict) and file_data.get("fileUri"):
                value = str(file_data["fileUri"])
                (asset_ids if value.startswith("asset:") else urls).append(value.removeprefix("asset:"))
            if isinstance(part.get("inlineData"), dict):
                raise HTTPException(status_code=413, detail="请先通过 /api/assets 上传图片，Gemini 兼容入口不接受大段 inlineData Base64")
    extension = payload.get("gflow") if isinstance(payload.get("gflow"), dict) else {}
    asset_ids.extend(str(item) for item in extension.get("input_asset_ids", []) if item)
    return "\n".join(texts).strip(), urls, asset_ids


async def _gemini_generate(model: str, payload: dict[str, Any], request: Request) -> dict[str, Any]:
    _check_gateway_auth(request)
    prompt, urls, asset_ids = _gemini_prompt(payload)
    if not prompt:
        raise HTTPException(status_code=400, detail="contents 中没有文本提示词")
    is_video = any(token in model.lower() for token in ("veo", "omni", "video"))
    extension = payload.get("gflow") if isinstance(payload.get("gflow"), dict) else {}
    generation = GenerationRequest(
        kind="video" if is_video else "image", prompt=prompt, model=model, profile=extension.get("profile"), project=extension.get("project"),
        aspect=str(extension.get("aspect") or "16:9"), duration=extension.get("duration"), mode=extension.get("mode") or "t2v",
        reference_images=urls, input_asset_ids=asset_ids, count=int(extension.get("count") or 1), wait=True,
    )
    task = await create_generation(generation)
    if task.get("status") != "succeeded":
        raise HTTPException(status_code=502, detail=task.get("error") or "生成失败")
    parts = []
    for item in (task.get("result") or {}).get("files", []) if isinstance(task.get("result"), dict) else []:
        if isinstance(item, dict) and item.get("preview_url"):
            parts.append({"fileData": {"mimeType": "video/mp4" if is_video else "image/png", "fileUri": item["preview_url"]}})
    return {"candidates": [{"content": {"role": "model", "parts": parts}, "finishReason": "STOP"}], "gflow_task_id": task["id"]}


@app.post("/v1beta/models/{model}:generateContent")
@app.post("/models/{model}:generateContent")
async def gemini_generate_content(model: str, payload: dict[str, Any], request: Request) -> dict[str, Any]:
    return await _gemini_generate(model, payload, request)


@app.post("/v1beta/models/{model}:streamGenerateContent")
@app.post("/models/{model}:streamGenerateContent")
async def gemini_stream_generate_content(model: str, payload: dict[str, Any], request: Request) -> StreamingResponse:
    result = await _gemini_generate(model, payload, request)

    async def stream():
        yield f"data: {json.dumps(result, ensure_ascii=False)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/health")
async def health() -> dict[str, Any]:
    import httpx

    mcp_reachable = False
    mcp_status = None
    try:
        async with httpx.AsyncClient(timeout=2, follow_redirects=False) as client:
            probe = await client.get(MCP_UPSTREAM_URL)
            mcp_reachable = True
            mcp_status = probe.status_code
    except httpx.HTTPError:
        pass
    return {
        "status": "ok",
        "service": "gflow-studio",
        "gflow_cli_src": str(GFLOW_SRC),
        "profiles": len(profile_store.list_profiles()),
        "queue": {"workers": QUEUE_MAX_WORKERS, "worker_online": _worker_is_online(), "active": sum(1 for item in TASK_HANDLES.values() if not item.done())},
        "auth_required": AUTH_REQUIRED,
        "role": STUDIO_ROLE,
        "mcp_upstream": {"reachable": mcp_reachable, "status_code": mcp_status},
        "timestamp": datetime.now(UTC).isoformat(),
    }


class StudioLoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=1, max_length=500)


@app.get("/api/auth/status")
async def studio_auth_status(request: Request) -> dict[str, Any]:
    authenticated = _is_loopback(request) and not AUTH_REQUIRED
    authenticated = authenticated or _valid_session(request.cookies.get("gflow_session")) or _valid_request_auth(request)
    return {"authenticated": authenticated, "required": AUTH_REQUIRED, "user": STUDIO_ADMIN_USER if authenticated else None}


@app.post("/api/auth/login")
async def studio_login(payload: StudioLoginRequest, response: Response) -> dict[str, Any]:
    if not STUDIO_ADMIN_PASSWORD:
        raise HTTPException(status_code=503, detail="未配置 GFLOW_STUDIO_ADMIN_PASSWORD")
    if not hmac.compare_digest(payload.username, STUDIO_ADMIN_USER) or not hmac.compare_digest(payload.password, STUDIO_ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="管理员账号或密码错误")
    expiry = int(time.time()) + 12 * 3600
    encoded_user = quote(STUDIO_ADMIN_USER, safe="")
    encoded_expiry = str(expiry)
    payload_to_sign = f"{encoded_user}.{encoded_expiry}"
    signature = hmac.new(STUDIO_SESSION_SECRET.encode(), payload_to_sign.encode(), hashlib.sha256).hexdigest()
    response.set_cookie("gflow_session", f"{payload_to_sign}.{signature}", max_age=12 * 3600, httponly=True, samesite="lax", secure=bool(PUBLIC_BASE_URL.startswith("https://")))
    return {"authenticated": True, "user": STUDIO_ADMIN_USER, "expires_at": expiry}


@app.post("/api/auth/logout")
async def studio_logout(response: Response) -> dict[str, Any]:
    response.delete_cookie("gflow_session")
    return {"authenticated": False}


@app.get("/api/models")
async def models() -> dict[str, Any]:
    catalog = build_catalog()
    image = []
    for item in catalog["image"]["models"]:
        aliases = item.get("aliases") or [item["name"]]
        image.append({"id": aliases[0], "name": item["name"], "aliases": aliases, "reference_cap": item["ref_cap"], "default": item["default"]})
    video = []
    for item in catalog["video"]["models"]:
        aliases = item.get("aliases") or [item["name"]]
        video.append({"id": aliases[0], "name": item["name"], "aliases": aliases, "reference_cap": item["ref_cap"], "max_duration": item["max_duration"]})
    return {"image": image, "video": video, "catalog": catalog, "source": "gflow-cli"}


class StudioConfigUpdateRequest(BaseModel):
    queue_workers: int | None = Field(default=None, ge=1, le=32)
    image_workers: int | None = Field(default=None, ge=1, le=32)
    video_workers: int | None = Field(default=None, ge=1, le=32)
    generation_timeout_seconds: int | None = Field(default=None, ge=60, le=86_400)
    media_url_ttl_seconds: int | None = Field(default=None, ge=300, le=30 * 86400)
    asset_retention_seconds: int | None = Field(default=None, ge=3600, le=90 * 86400)
    public_base_url: str | None = Field(default=None, max_length=500)
    mcp_upstream_url: str | None = Field(default=None, max_length=500)
    generation_rate_capacity: int | None = Field(default=None, ge=1, le=1000)
    generation_rate_refill_seconds: float | None = Field(default=None, ge=0.1, le=86400)


@app.get("/api/config")
async def studio_config() -> dict[str, Any]:
    return {
        "queue_workers": QUEUE_MAX_WORKERS,
        "worker_online": _worker_is_online(),
        "image_workers": IMAGE_MAX_WORKERS,
        "video_workers": VIDEO_MAX_WORKERS,
        "generation_timeout_seconds": GENERATION_TIMEOUT_SECONDS,
        "media_url_ttl_seconds": MEDIA_URL_TTL_SECONDS,
        "asset_retention_seconds": ASSET_RETENTION_SECONDS,
        "asset_max_bytes": ASSET_MAX_BYTES,
        "asset_max_total_bytes": ASSET_MAX_TOTAL_BYTES,
        "rate_limit_per_minute": RATE_LIMIT_PER_MINUTE,
        "public_base_url": PUBLIC_BASE_URL,
        "mcp_upstream_url": MCP_UPSTREAM_URL,
        "generation_rate_capacity": GENERATION_RATE_CAPACITY,
        "generation_rate_refill_seconds": GENERATION_RATE_REFILL_SECONDS,
        "auth_required": AUTH_REQUIRED,
        "mcp_endpoint": f"{PUBLIC_BASE_URL}/mcp" if PUBLIC_BASE_URL else "/mcp",
    }


@app.put("/api/config")
async def update_studio_config(payload: StudioConfigUpdateRequest) -> dict[str, Any]:
    global QUEUE_MAX_WORKERS, IMAGE_MAX_WORKERS, VIDEO_MAX_WORKERS, GENERATION_TIMEOUT_SECONDS, MEDIA_URL_TTL_SECONDS, ASSET_RETENTION_SECONDS, PUBLIC_BASE_URL, MCP_UPSTREAM_URL, GENERATION_RATE_CAPACITY, GENERATION_RATE_REFILL_SECONDS
    values = payload.model_dump(exclude_none=True)
    if "public_base_url" in values:
        PUBLIC_BASE_URL = str(values["public_base_url"]).rstrip("/")
    if "mcp_upstream_url" in values:
        MCP_UPSTREAM_URL = str(values["mcp_upstream_url"]).rstrip("/")
    if "queue_workers" in values:
        QUEUE_MAX_WORKERS = int(values["queue_workers"])
    if "image_workers" in values:
        IMAGE_MAX_WORKERS = int(values["image_workers"])
    if "video_workers" in values:
        VIDEO_MAX_WORKERS = int(values["video_workers"])
    if "generation_timeout_seconds" in values:
        GENERATION_TIMEOUT_SECONDS = int(values["generation_timeout_seconds"])
    if "media_url_ttl_seconds" in values:
        MEDIA_URL_TTL_SECONDS = int(values["media_url_ttl_seconds"])
    if "asset_retention_seconds" in values:
        ASSET_RETENTION_SECONDS = int(values["asset_retention_seconds"])
    if "generation_rate_capacity" in values:
        GENERATION_RATE_CAPACITY = int(values["generation_rate_capacity"])
    if "generation_rate_refill_seconds" in values:
        GENERATION_RATE_REFILL_SECONDS = float(values["generation_rate_refill_seconds"])
    if "generation_rate_capacity" in values or "generation_rate_refill_seconds" in values:
        configure_rate_limiter(GENERATION_RATE_CAPACITY, GENERATION_RATE_REFILL_SECONDS)
    _save_studio_settings(values)
    QUEUE_WAKE.set()
    return await studio_config()


@app.get("/api/stats")
async def stats() -> dict[str, Any]:
    """Return cheap, local dashboard counters without opening browser sessions."""
    _refresh_task_cache()
    items = _task_snapshots()
    discovered = profile_store.list_profiles()
    known_names = {item.name for item in discovered} | _profile_setting_names()
    enabled_names = {name for name in known_names if _profile_setting(name)["enabled"]}
    pool_names = {item.name for item in _pool_profiles() if item.name in enabled_names}
    busy_names = {
        str(item.get("profile"))
        for item in items
        if item.get("profile") in pool_names and item.get("status") in {"queued", "running", "cancelling"}
    }
    now = time.time()
    queued_items = [item for item in items if item.get("status") == "queued"]
    running_items = [item for item in items if item.get("status") in {"running", "cancelling"}]
    return {
        "profiles": len(known_names),
        "enabled_profiles": len(enabled_names),
        "available_profiles": len(pool_names),
        "busy_profiles": len(busy_names),
        "worker_online": _worker_is_online(),
        "tasks": len(items),
        "queued": sum(item.get("status") == "queued" for item in items),
        "running": sum(item.get("status") in {"running", "cancelling"} for item in items),
        "oldest_queued_age_seconds": max((_task_age_seconds(item, now) for item in queued_items), default=0.0),
        "oldest_running_age_seconds": max((_task_age_seconds(item, now) for item in running_items), default=0.0),
        "completed": sum(item.get("status") == "succeeded" for item in items),
        "failed": sum(item.get("status") in {"failed", "timed_out", "cancelled", "indeterminate", "interrupted"} for item in items),
        "interrupted": sum(item.get("status") == "interrupted" for item in items),
        "timed_out": sum(item.get("status") == "timed_out" for item in items),
        "cancelled": sum(item.get("status") == "cancelled" for item in items),
        "queue_workers": QUEUE_MAX_WORKERS,
        "image_workers": IMAGE_MAX_WORKERS,
        "video_workers": VIDEO_MAX_WORKERS,
        "image_account_capacity": _pool_capacity("image"),
        "video_account_capacity": _pool_capacity("video"),
        "generation_timeout_seconds": GENERATION_TIMEOUT_SECONDS,
        "timestamp": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics")
async def metrics(request: Request) -> PlainTextResponse:
    _check_gateway_auth(request)
    snapshot = await stats()
    lines = [
        "# HELP gflow_studio_profiles_total Known profiles.",
        "# TYPE gflow_studio_profiles_total gauge",
        f"gflow_studio_profiles_total {snapshot['profiles']}",
        f"gflow_studio_profiles_available {snapshot['available_profiles']}",
        "# HELP gflow_studio_worker_online Whether a worker heartbeat is fresh.",
        "# TYPE gflow_studio_worker_online gauge",
        f"gflow_studio_worker_online {1 if snapshot['worker_online'] else 0}",
        f"gflow_studio_tasks_queued {snapshot['queued']}",
        f"gflow_studio_tasks_running {snapshot['running']}",
        f"gflow_studio_tasks_completed {snapshot['completed']}",
        f"gflow_studio_tasks_failed {snapshot['failed']}",
        "# HELP gflow_studio_oldest_queued_age_seconds Age of the oldest persisted queued task.",
        "# TYPE gflow_studio_oldest_queued_age_seconds gauge",
        f"gflow_studio_oldest_queued_age_seconds {snapshot['oldest_queued_age_seconds']:.3f}",
        "# HELP gflow_studio_oldest_running_age_seconds Age of the oldest persisted running task.",
        "# TYPE gflow_studio_oldest_running_age_seconds gauge",
        f"gflow_studio_oldest_running_age_seconds {snapshot['oldest_running_age_seconds']:.3f}",
        "# HELP gflow_studio_generation_timeout_seconds Configured generation timeout.",
        "# TYPE gflow_studio_generation_timeout_seconds gauge",
        f"gflow_studio_generation_timeout_seconds {snapshot['generation_timeout_seconds']}",
        f"gflow_studio_queue_workers {snapshot['queue_workers']}",
        f"gflow_studio_image_account_capacity {snapshot['image_account_capacity']}",
        f"gflow_studio_video_account_capacity {snapshot['video_account_capacity']}",
    ]
    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/api/profiles")
async def profiles() -> dict[str, Any]:
    _refresh_task_cache(all_queue_tasks=True)
    discovered = profile_store.list_profiles()
    items = [_profile_payload(item) for item in discovered]
    discovered_names = {item.name for item in discovered}
    default_name = profile_store.get_default_profile()
    for name in sorted(_profile_setting_names() - discovered_names):
        setting = _profile_setting(name)
        items.append({
            "name": name,
            "google_account": None,
            "cookies_present": False,
            "is_default": name == default_name,
            "last_used_at": None,
            "remark": setting["remark"],
            "enabled": setting["enabled"],
            "image_concurrency": setting["image_concurrency"],
            "video_concurrency": setting["video_concurrency"],
            "effective_image_concurrency": 1,
            "effective_video_concurrency": 1,
            "auth_state": "unknown",
            "auth_checked_at": None,
            "auth_error": "",
            "auth_failure_count": 0,
            "pending_login": True,
        })
    items.sort(key=lambda item: (not item["is_default"], item["name"]))
    return {"profiles": items, "count": len(items)}


async def _start_login(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(GFLOW_SRC) + os.pathsep + env.get("PYTHONPATH", "")
    command = [
        sys.executable,
        "-m",
        "gflow_cli.cli",
        "auth",
        "login",
        "--browser",
        "chrome",
        "--profile",
        name,
    ]
    flags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
    process = subprocess.Popen(command, cwd=str(GFLOW_SRC.parent.parent), env=env, creationflags=flags)
    _upsert_profile_setting(name)
    _reset_profile_health(name)
    return {"status": "started", "profile": name, "pid": process.pid}


@app.post("/api/profiles", status_code=202)
async def create_profile(request: ProfileCreateRequest) -> dict[str, Any]:
    name = _validate_profile(request.name)
    if any(item.name == name for item in profile_store.list_profiles()):
        raise HTTPException(status_code=409, detail="Profile 已存在，请使用重新登录")
    _upsert_profile_setting(
        name,
        remark=request.remark,
        enabled=True,
        image_concurrency=request.image_concurrency,
        video_concurrency=request.video_concurrency,
    )
    return await _start_login(name)


@app.put("/api/profiles/{name}")
async def update_profile(name: str, request: ProfileUpdateRequest) -> dict[str, Any]:
    name = _validate_profile(name)
    existing = next((item for item in profile_store.list_profiles() if item.name == name), None)
    if existing is None and name not in _profile_setting_names():
        raise HTTPException(status_code=404, detail="Profile 不存在")
    target = name
    if request.new_name and request.new_name != name:
        target = _validate_profile(request.new_name)
        if target in _known_profile_names():
            raise HTTPException(status_code=409, detail="新的 Profile 名称已存在")
        active = [task for task in TASKS.values() if task.get("profile") == name and task.get("status") in {"queued", "running", "cancelling"}]
        if active:
            raise HTTPException(status_code=409, detail="该 Profile 有运行中的任务，暂时不能改名")
        old_setting = _profile_setting(name)
        if existing is not None:
            try:
                profile_store.rename_profile(name, target)
            except (FileNotFoundError, FileExistsError, ValueError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        _delete_profile_setting(name)
        _upsert_profile_setting(
            target,
            remark=old_setting["remark"],
            enabled=old_setting["enabled"],
            image_concurrency=old_setting["image_concurrency"],
            video_concurrency=old_setting["video_concurrency"],
        )
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("UPDATE profile_projects SET profile_name = ? WHERE profile_name = ?", (target, name))
            conn.execute("UPDATE profile_health SET profile_name = ? WHERE profile_name = ?", (target, name))
            conn.commit()
    _upsert_profile_setting(
        target,
        remark=request.remark,
        enabled=request.enabled,
        image_concurrency=request.image_concurrency,
        video_concurrency=request.video_concurrency,
    )
    updated = next((item for item in profile_store.list_profiles() if item.name == target), None)
    if updated is not None:
        payload = _profile_payload(updated)
    else:
        setting = _profile_setting(target)
        payload = {
            "name": target,
            "google_account": None,
            "cookies_present": False,
            "is_default": False,
            "last_used_at": None,
            "remark": setting["remark"],
            "enabled": setting["enabled"],
            "image_concurrency": setting["image_concurrency"],
            "video_concurrency": setting["video_concurrency"],
            "effective_image_concurrency": 1,
            "effective_video_concurrency": 1,
            "image_inflight": 0,
            "video_inflight": 0,
            "queued_tasks": 0,
            "pending_login": True,
        }
    return {"status": "updated", "profile": payload}


@app.delete("/api/profiles/{name}")
async def delete_profile(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    active = [task for task in TASKS.values() if task.get("profile") == name and task.get("status") in {"queued", "running", "cancelling"}]
    if active:
        raise HTTPException(status_code=409, detail="该 Profile 有未完成任务，请先取消或等待完成")
    has_real_profile = any(item.name == name for item in profile_store.list_profiles())
    has_setting = name in _profile_setting_names()
    if not has_real_profile and not has_setting:
        raise HTTPException(status_code=404, detail="Profile 不存在")
    deleted_path = None
    if has_real_profile:
        try:
            deleted_path = profile_store.delete_profile(name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Profile 不存在") from exc
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM profile_projects WHERE profile_name = ?", (name,))
        conn.execute("DELETE FROM profile_health WHERE profile_name = ?", (name,))
        conn.commit()
    _delete_profile_setting(name)
    return {"status": "deleted", "profile": name, "path": str(deleted_path) if deleted_path else None, "pending_login": not has_real_profile}


@app.post("/api/profiles/{name}/login")
async def start_login(name: str) -> dict[str, Any]:
    """Start interactive login in a separate local console.

    The browser interaction remains visible and user-driven; the web UI never
    receives Google credentials or cookies.
    """
    return await _start_login(name)


@app.get("/api/profiles/{name}/status")
async def profile_auth_status(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    health = _profile_health(name)
    result = {
        "status": "not_probed",
        "profile": name,
        "message": "旧版主动鉴权已停用；账号将在真实生成任务中验证。",
    }
    return {"profile": name, "result": result, "health": health}


@app.get("/api/profiles/{name}/credits")
async def profile_credits(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    result = await gflow_get_credits(profile=name)
    return {"profile": name, "result": result}


@app.get("/api/projects")
async def projects(profile: str = "default", limit: int = 50) -> dict[str, Any]:
    profile = _validate_profile(profile)
    result = await gflow_list_projects(profile=profile, limit=max(1, min(limit, 200)))
    return {"profile": profile, "result": result}


class ProjectRequest(BaseModel):
    title: str = Field(default="gflow studio project", min_length=1, max_length=120)
    profile: str = "default"


class ProfileProjectRequest(BaseModel):
    project_id: str
    title: str = Field(default="", max_length=200)
    enabled: bool = True
    is_default: bool = False


class ProfileProjectUpdateRequest(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    enabled: bool | None = None
    is_default: bool | None = None


async def _catalog_projects(profile: str) -> list[dict[str, Any]]:
    result = await gflow_list_projects(profile=profile, limit=200)
    if not isinstance(result, dict):
        return []
    projects = result.get("projects")
    return [item for item in projects if isinstance(item, dict) and item.get("project_id")] if isinstance(projects, list) else []


async def _resolve_project(profile: str, requested: str | None) -> str | None:
    """Resolve a project from the per-profile pool, lazily importing catalog data."""
    if requested:
        project_id = _validate_project_id(requested)
        catalog = await _catalog_projects(profile)
        catalog_item = next((item for item in catalog if item.get("project_id") == project_id), None)
        _upsert_profile_project(profile, project_id, title=str((catalog_item or {}).get("title") or ""), enabled=True)
        _mark_project_used(profile, project_id)
        return project_id

    all_rows = _profile_project_rows(profile)
    configured = [item for item in all_rows if item["enabled"]]
    if not all_rows:
        catalog = await _catalog_projects(profile)
        for item in catalog:
            try:
                _upsert_profile_project(
                    profile,
                    str(item["project_id"]),
                    title=str(item.get("title") or ""),
                    enabled=False,
                )
            except (KeyError, ValueError):
                continue
        configured = _profile_project_rows(profile, enabled_only=True)
        if not configured and catalog:
            first = catalog[0]
            project_id = _validate_project_id(str(first["project_id"]))
            _upsert_profile_project(profile, project_id, title=str(first.get("title") or ""), enabled=True, is_default=True)
            configured = _profile_project_rows(profile, enabled_only=True)
    if not configured:
        return None
    defaults = [item for item in configured if item["is_default"]]
    selected = defaults[0] if defaults else min(configured, key=lambda item: (item["use_count"], item["last_used_at"] or "", item["project_id"]))
    _mark_project_used(profile, selected["project_id"])
    return selected["project_id"]


@app.post("/api/projects", status_code=201)
async def create_project(request: ProjectRequest) -> dict[str, Any]:
    profile = _validate_profile(request.profile)
    settings = get_settings()
    async with _profile_lock(profile):
        try:
            async with FlowApiClient(
                profile_dir=settings.profile_subdir(profile),
                headless=settings.headless,
            ) as client:
                project = await client.create_project(title=request.title)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Flow 项目创建失败：{type(exc).__name__}") from exc
    has_default = any(item["is_default"] for item in _profile_project_rows(profile))
    pool_item = _upsert_profile_project(
        profile,
        project.project_id,
        title=project.title,
        enabled=True,
        is_default=not has_default,
    )
    return {"status": "created", "profile": profile, "project": pool_item}


@app.get("/api/profiles/{name}/projects")
async def profile_projects(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    catalog = await _catalog_projects(name)
    known = {item["project_id"] for item in _profile_project_rows(name)}
    for item in catalog:
        project_id = str(item.get("project_id") or "")
        if project_id and project_id not in known:
            try:
                _upsert_profile_project(name, project_id, title=str(item.get("title") or ""), enabled=False)
            except ValueError:
                continue
    return {"profile": name, "projects": _profile_project_rows(name), "catalog": catalog}


@app.post("/api/profiles/{name}/projects")
async def add_profile_project(name: str, request: ProfileProjectRequest) -> dict[str, Any]:
    name = _validate_profile(name)
    if not any(item.name == name for item in profile_store.list_profiles()):
        raise HTTPException(status_code=404, detail="Profile 不存在")
    try:
        item = _upsert_profile_project(
            name,
            request.project_id,
            title=request.title,
            enabled=request.enabled,
            is_default=request.is_default,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "added", "project": item}


@app.put("/api/profiles/{name}/projects/{project_id}")
async def update_profile_project(name: str, project_id: str, request: ProfileProjectUpdateRequest) -> dict[str, Any]:
    name = _validate_profile(name)
    try:
        project_id = _validate_project_id(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    current = next((item for item in _profile_project_rows(name) if item["project_id"] == project_id), None)
    if current is None:
        raise HTTPException(status_code=404, detail="项目不在账号池中")
    item = _upsert_profile_project(
        name,
        project_id,
        title=current["title"] if request.title is None else request.title,
        enabled=current["enabled"] if request.enabled is None else request.enabled,
        is_default=current["is_default"] if request.is_default is None else request.is_default,
    )
    return {"status": "updated", "project": item}


@app.delete("/api/profiles/{name}/projects/{project_id}")
async def delete_profile_project(name: str, project_id: str) -> dict[str, Any]:
    name = _validate_profile(name)
    try:
        project_id = _validate_project_id(project_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    current = next((item for item in _profile_project_rows(name) if item["project_id"] == project_id), None)
    if current is None or not _delete_profile_project(name, project_id):
        raise HTTPException(status_code=404, detail="项目不在账号池中")
    if current["is_default"]:
        remaining = _profile_project_rows(name, enabled_only=True)
        if remaining:
            _upsert_profile_project(name, remaining[0]["project_id"], is_default=True)
    return {"status": "deleted", "profile": name, "project_id": project_id}


@app.post("/api/profiles/{name}/default")
async def set_default_profile(name: str) -> dict[str, Any]:
    name = _validate_profile(name)
    item = next((item for item in profile_store.list_profiles() if item.name == name), None)
    if item is None or not item.cookies_present:
        raise HTTPException(status_code=409, detail="只有已登录 Profile 才能设为默认账号")
    if not _profile_setting(name)["enabled"]:
        raise HTTPException(status_code=409, detail="禁用的 Profile 不能设为默认账号")
    try:
        profile_store.set_default_profile(name)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", "profile": name}


@app.post("/api/assets")
async def upload_asset(file: UploadFile = File(...)) -> dict[str, Any]:
    """Store a local reference image; generation receives its path, not Base64."""
    suffix = Path(file.filename or "asset.bin").suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
        raise HTTPException(status_code=400, detail="只支持 PNG、JPG、JPEG、WEBP 图片")
    target = UPLOAD_DIR / f"{uuid.uuid4().hex}{suffix}"
    total = 0
    digest = hashlib.sha256()
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        current_total = conn.execute("SELECT COALESCE(SUM(bytes), 0) FROM studio_assets WHERE expires_at > ?", (datetime.now(UTC).isoformat(),)).fetchone()[0]
    if current_total >= ASSET_MAX_TOTAL_BYTES:
        raise HTTPException(status_code=413, detail="输入素材存储已达到上限，请先清理过期素材")
    with target.open("wb") as handle:
        while chunk := await file.read(1024 * 1024):
            total += len(chunk)
            if total > ASSET_MAX_BYTES or current_total + total > ASSET_MAX_TOTAL_BYTES:
                target.unlink(missing_ok=True)
                message = f"单张参考图不能超过 {ASSET_MAX_BYTES // 1024 // 1024}MB" if total > ASSET_MAX_BYTES else "输入素材存储已达到上限"
                raise HTTPException(status_code=413, detail=message)
            digest.update(chunk)
            handle.write(chunk)
    sha256 = digest.hexdigest()
    with sqlite3.connect(DB_PATH) as conn:
        existing = conn.execute("SELECT asset_id, local_path FROM studio_assets WHERE sha256 = ? AND expires_at > ?", (sha256, datetime.now(UTC).isoformat())).fetchone()
    if existing and Path(existing[1]).is_file():
        target.unlink(missing_ok=True)
        return _public_asset(_asset_record(str(existing[0])) or {"asset_id": str(existing[0]), "path": existing[1]})
    return _public_asset(_save_asset_record(asset_id=target.stem, filename=file.filename or target.name, path=target, size=total, sha256=sha256))


class AssetImportRequest(BaseModel):
    url: str = Field(min_length=1, max_length=2000)
    filename: str | None = Field(default=None, max_length=200)


def _validate_public_asset_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise HTTPException(status_code=400, detail="远程输入资产只允许不带账号信息的 HTTPS URL")
    try:
        addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise HTTPException(status_code=400, detail="远程资产域名无法解析") from exc
    for entry in addresses:
        address = ipaddress.ip_address(entry[4][0])
        if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
            raise HTTPException(status_code=400, detail="远程资产 URL 不允许指向内网或本机地址")


@app.post("/api/assets/import")
async def import_asset(payload: AssetImportRequest) -> dict[str, Any]:
    await asyncio.to_thread(_validate_public_asset_url, payload.url)
    import httpx

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=False) as client:
            response = await client.get(payload.url)
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"远程资产下载失败：{type(exc).__name__}") from exc
    if response.is_redirect or response.is_permanent_redirect:
        raise HTTPException(status_code=400, detail="远程资产 URL 不允许跳转，请使用最终 HTTPS 地址")
    content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
    suffix = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}.get(content_type, Path(payload.url.split("?", 1)[0]).suffix.lower())
    if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
        raise HTTPException(status_code=400, detail="远程 URL 必须返回 PNG、JPG 或 WEBP 图片")
    if len(response.content) > ASSET_MAX_BYTES:
        raise HTTPException(status_code=413, detail=f"单张参考图不能超过 {ASSET_MAX_BYTES // 1024 // 1024}MB")
    _init_db()
    with sqlite3.connect(DB_PATH) as conn:
        current_total = conn.execute("SELECT COALESCE(SUM(bytes), 0) FROM studio_assets WHERE expires_at > ?", (datetime.now(UTC).isoformat(),)).fetchone()[0]
    if current_total + len(response.content) > ASSET_MAX_TOTAL_BYTES:
        raise HTTPException(status_code=413, detail="输入素材存储已达到上限，请先清理过期素材")
    digest = hashlib.sha256(response.content).hexdigest()
    with sqlite3.connect(DB_PATH) as conn:
        existing = conn.execute("SELECT asset_id, local_path FROM studio_assets WHERE sha256 = ? AND expires_at > ?", (digest, datetime.now(UTC).isoformat())).fetchone()
    if existing and Path(existing[1]).is_file():
        return _public_asset(_asset_record(str(existing[0])) or {"asset_id": str(existing[0]), "path": existing[1]})
    target = UPLOAD_DIR / f"{uuid.uuid4().hex}{suffix}"
    target.write_bytes(response.content)
    return _public_asset(_save_asset_record(asset_id=target.stem, filename=payload.filename or target.name, path=target, size=len(response.content), sha256=digest))


@app.get("/api/assets")
async def assets(profile: str | None = None, kind: Literal["all", "image", "video"] = "all", limit: int = 50) -> dict[str, Any]:
    """Return catalogued generated assets with safe local preview URLs."""
    settings = get_settings()
    db_path = settings.resolved_db_path()
    limit = max(1, min(limit, 200))
    rows: list[Any] = []
    if kind in {"all", "image"}:
        rows.extend(list_images(db_path=db_path, profile=profile, limit=limit, offset=0))
    if kind in {"all", "video"}:
        rows.extend(list_videos(db_path=db_path, profile=profile, limit=limit, offset=0))
    rows.sort(key=lambda item: item.created_at, reverse=True)
    payload = []
    for row in rows[:limit]:
        item = {
            "media_id": row.media_id,
            "profile": row.profile,
            "project_id": row.project_id,
            "prompt": row.prompt,
            "aspect": row.aspect,
            "model": row.model,
            "created_at": row.created_at.isoformat(),
            "preview_url": _media_url(str(row.local_path)) if row.local_path else None,
        }
        if hasattr(row, "duration"):
            item["duration"] = row.duration
        payload.append(item)
    with sqlite3.connect(DB_PATH) as conn:
        input_rows = conn.execute("SELECT asset_id, filename, local_path, bytes, sha256, created_at, expires_at FROM studio_assets ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    inputs = [{"asset_id": row[0], "filename": row[1], "bytes": row[3], "sha256": row[4], "created_at": row[5], "expires_at": row[6], "preview_url": _media_url(row[2])} for row in input_rows if Path(row[2]).is_file()]
    return {"assets": payload, "input_assets": inputs, "count": len(payload), "input_count": len(inputs)}


@app.get("/api/logs")
async def logs(
    profile: str | None = None,
    status: str | None = None,
    q: str | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Return one normalized, filterable view over studio and gflow failures."""
    _refresh_task_cache()
    settings = get_settings()
    limit = max(1, min(limit, 200))
    live_tasks = [_public_log(item) for item in _task_snapshots(10_000) if profile is None or item.get("profile") == profile]
    failure_rows = list_errors(
        db_path=settings.resolved_db_path(),
        profile=profile,
        limit=10_000,
        offset=0,
    )
    persisted = [_gflow_error_log(row) for row in failure_rows]
    merged = sorted([*live_tasks, *persisted, *_event_logs(profile=profile, limit=10_000)], key=lambda item: item.get("created_at", ""), reverse=True)
    if status:
        merged = [item for item in merged if item.get("status") == status]
    if q:
        needle = q.lower()
        merged = [item for item in merged if needle in json.dumps(item, ensure_ascii=False, default=str).lower()]
    if since:
        merged = [item for item in merged if str(item.get("created_at") or "") >= since]
    if until:
        merged = [item for item in merged if str(item.get("created_at") or "") <= until]
    offset = max(0, offset)
    return {"logs": merged[offset:offset + limit], "count": len(merged), "limit": limit, "offset": offset}


@app.get("/api/logs/export")
async def export_logs(request: Request, format: Literal["json", "csv"] = "json") -> Response:
    _check_gateway_auth(request)
    payload = await logs(limit=10_000)
    if format == "json":
        return JSONResponse(payload)
    rows = payload["logs"]
    columns = ["id", "created_at", "finished_at", "source", "profile", "kind", "model", "status", "error"]
    output = __import__("io").StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return PlainTextResponse(output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=gflow-logs.csv"})


def _public_log(task: dict[str, Any]) -> dict[str, Any]:
    public = _public_task(task)
    request = task.get("request")
    if isinstance(request, dict):
        safe_request = dict(request)
        safe_request["reference_images"] = [Path(str(item)).name for item in safe_request.get("reference_images", [])]
        public["request"] = safe_request
    public["source"] = "gflow-studio"
    return public


def _gflow_error_id(row: Any) -> str:
    raw = "\x1f".join(
        str(getattr(row, field, "") or "")
        for field in ("started_at", "completed_at", "profile", "command", "mode", "model", "error_type", "error_detail")
    )
    return f"flow-error-{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _gflow_error_log(row: Any) -> dict[str, Any]:
    return {
        "id": _gflow_error_id(row),
        "source": "gflow-cli",
        "status": "failed",
        "profile": row.profile,
        "kind": row.mode,
        "model": row.model,
        "command": row.command,
        "error_type": row.error_type,
        "error": row.error_detail or row.error_type,
        "created_at": row.started_at.isoformat(),
        "finished_at": row.completed_at.isoformat() if row.completed_at else None,
    }


@app.get("/api/logs/{log_id}")
async def log_detail(log_id: str) -> dict[str, Any]:
    task = _load_task(log_id)
    if task is not None:
        return {"log": _public_log(task)}
    if log_id.startswith("flow-error-"):
        rows = list_errors(db_path=get_settings().resolved_db_path(), profile=None, limit=200, offset=0)
        row = next((item for item in rows if _gflow_error_id(item) == log_id), None)
        if row is None:
            raise HTTPException(status_code=404, detail="日志不存在")
        return {"log": _gflow_error_log(row)}
    event = next((item for item in _event_logs(limit=10_000) if item["id"] == log_id), None)
    if event is not None:
        return {"log": event}
    raise HTTPException(status_code=404, detail="日志不存在")


@app.delete("/api/logs")
async def clear_logs() -> dict[str, Any]:
    """Clear local studio task logs; the gflow-cli catalog remains intact."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM studio_tasks")
        conn.execute("DELETE FROM studio_events")
        conn.commit()
    for task_id in list(TASKS):
        if task_id not in TASK_HANDLES:
            TASKS.pop(task_id, None)
    return {"status": "cleared", "scope": "gflow-studio"}


async def _execute_generation(task_id: str, request: GenerationRequest) -> None:
    task = TASKS[task_id]
    task["status"] = "running"
    task["started_at"] = datetime.now(UTC).isoformat()
    _set_task_progress(task, 5, "选择账号")
    try:
        if task.get("cancel_requested") or _task_cancel_requested(task_id):
            task["status"] = "cancelled"
            task["error"] = "任务在提交到 Flow 前已取消"
            return
        candidates = _execution_candidates(request.profile)
        last_auth_error = "账号池中没有可用的已登录账号"
        generation_finished = False
        for profile in candidates:
            lock = _profile_lock(profile)
            async with lock:
                if task.get("cancel_requested"):
                    task["status"] = "cancelled"
                    task["error"] = "任务在提交到 Flow 前已取消"
                    generation_finished = True
                    break
                if not _profile_setting(profile)["enabled"]:
                    last_auth_error = f"Profile 已禁用：{profile}"
                    continue
                task["profile"] = profile
                task["assigned_profile"] = profile
                _set_task_progress(task, 15, "账号已选择，提交真实任务")
                try:
                    selected_project = await _resolve_project(profile, request.project)
                except Exception as exc:
                    last_auth_error = f"Profile {profile} 项目池不可用：{type(exc).__name__}"
                    if (request.profile or "auto") != "auto":
                        break
                    continue
                task["project"] = selected_project
                task["assigned_project"] = selected_project
                _set_task_progress(task, 22, "项目已选择")
                try:
                    if _task_cancel_requested(task_id):
                        task["status"] = "cancelled"
                        task["error"] = "任务在提交到 Flow 前已取消"
                        generation_finished = True
                        break
                    _set_task_progress(task, 30, "提交到 Flow")

                    def on_flow_checkpoint(checkpoint: Any) -> None:
                        phase = str(getattr(checkpoint, "phase", "upstream"))
                        phase_progress = {
                            "submit_attempted": 45,
                            "remote_started": 70,
                        }
                        task["upstream_phase"] = phase
                        operation_id = getattr(checkpoint, "operation_id", None)
                        media_ids = getattr(checkpoint, "media_ids", ())
                        workflow_ids = getattr(checkpoint, "workflow_ids", ())
                        if operation_id:
                            task["upstream_operation_id"] = str(operation_id)
                        if media_ids:
                            task["upstream_media_ids"] = [str(value) for value in media_ids]
                        if workflow_ids:
                            task["upstream_workflow_ids"] = [str(value) for value in workflow_ids]
                        _set_task_progress(
                            task,
                            phase_progress.get(phase, max(30, int(task.get("progress", 30)))),
                            f"Flow：{phase}",
                        )

                    progress_token = set_generation_progress_callback(on_flow_checkpoint)
                    try:
                        async with asyncio.timeout(request.timeout_seconds):
                            if request.kind == "image":
                                result = await gflow_generate_image(
                                    prompt=request.prompt,
                                    model=request.model or "nano-pro",
                                    aspect=request.aspect,
                                    count=request.count,
                                    reference_images=request.reference_images or None,
                                    profile=profile,
                                    project=selected_project,
                                    wait=True,
                                )
                            else:
                                result = await gflow_generate_video(
                                    prompt=request.prompt,
                                    mode=request.mode,
                                    aspect=request.aspect,
                                    initial_frame=request.initial_frame,
                                    end_frame=request.end_frame,
                                    reference_images=request.reference_images or None,
                                    model=request.model,
                                    duration=request.duration,
                                    count=request.count,
                                    profile=profile,
                                    project=selected_project,
                                    wait=True,
                                )
                    finally:
                        reset_generation_progress_callback(progress_token)
                except TimeoutError:
                    task["status"] = "timed_out"
                    task["error"] = f"任务超过 {request.timeout_seconds} 秒仍未完成；提交是否已扣费未知，请在 Flow 项目中核对"
                    task["error_code"] = "TASK_TIMEOUT"
                    generation_finished = True
                    break
                if _task_cancel_requested(task_id):
                    task["status"] = "indeterminate"
                    task["error"] = "任务已请求取消，但浏览器调用已进入提交阶段；请在 Flow 项目中核对是否产生资产"
                    task["error_code"] = "CANCEL_AFTER_SUBMIT"
                    generation_finished = True
                    break
                result = _materialize_result_files(result, profile=profile, kind=request.kind)
                task["result"] = result
                upstream_succeeded = result.get("status") in {"ok", "succeeded", "completed"}
                has_materialized_result = bool(result.get("files"))
                task["status"] = "succeeded" if upstream_succeeded and has_materialized_result else "failed"
                _set_task_progress(task, 95 if task["status"] == "succeeded" else 100, "下载结果" if task["status"] == "succeeded" else "任务结束")
                if task["status"] == "failed":
                    task["error"] = _error_detail(result) if not upstream_succeeded else "上游返回成功，但没有可下载的结果资产"
                    if upstream_succeeded and not has_materialized_result:
                        task["error_code"] = "RESULT_NOT_MATERIALIZED"
                    error = result.get("error") if isinstance(result, dict) else None
                    if _is_auth_failure(result):
                        _record_profile_health(profile, {"status": "error", "error": error if isinstance(error, dict) else {"message": _error_detail(result)}})
                        last_auth_error = f"Profile {profile} 鉴权失败：{task['error']}"
                        if (request.profile or "auto") == "auto":
                            task["status"] = "running"
                            continue
                generation_finished = True
                if task["status"] == "succeeded":
                    _set_task_progress(task, 100, "已完成")
                break
        else:
            task["status"] = "failed"
            task["error"] = last_auth_error
        if not generation_finished and task["status"] == "running":
            task["status"] = "failed"
            task["error"] = last_auth_error
    except asyncio.CancelledError:
        # Cancellation after submit is deliberately surfaced as indeterminate.
        if not SHUTTING_DOWN:
            task["status"] = "indeterminate"
            task["error"] = "任务被取消；提交是否已扣费未知，请在 Flow 项目中核对"
        raise
    except Exception as exc:  # gflow's own envelope is preserved above
        task["status"] = "failed"
        task["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        task["finished_at"] = datetime.now(UTC).isoformat()
        _persist_task(task)
        TASK_HANDLES.pop(task_id, None)


@app.post("/api/generations", status_code=202)
async def create_generation(request: GenerationRequest) -> dict[str, Any]:
    if request.input_asset_ids:
        request = request.model_copy(update={"reference_images": [*request.reference_images, *_resolve_input_asset_ids(request.input_asset_ids)]})
    if request.kind == "video" and _veo_model_has_no_duration_control(request.model) and request.duration is not None:
        request = request.model_copy(update={"duration": None})
    requested_profile = request.profile or "auto"
    if requested_profile != "auto":
        _validate_profile(requested_profile)
        if not _profile_setting(requested_profile)["enabled"]:
            raise HTTPException(status_code=409, detail="该 Profile 已禁用，请先启用后再测试")
        if not any(item.name == requested_profile and item.cookies_present for item in profile_store.list_profiles()):
            raise HTTPException(status_code=409, detail="该 Profile 尚未完成登录")
    elif not _pool_profiles():
        raise HTTPException(status_code=409, detail="账号池没有可用账号，请先添加并登录至少一个启用的 Profile")
    if request.kind == "video" and request.mode in {"i2v", "r2v"} and not (
        request.initial_frame or request.reference_images
    ):
        raise HTTPException(status_code=400, detail="图生视频/参考生视频需要参考资产")
    request_hash = _request_hash(request)
    idempotency_key = request.idempotency_key
    _refresh_task_cache()
    existing = _find_idempotent(idempotency_key, request_hash)
    if existing is not None:
        return _public_task(existing)
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "id": task_id,
        "profile": requested_profile,
        "kind": request.kind,
        "model": request.model,
        "requested_model": request.model,
        "prompt": request.prompt,
        "project": request.project,
        "status": "queued",
        "progress": 0,
        "phase": "排队等待",
        "created_at": datetime.now(UTC).isoformat(),
        "available_at": datetime.now(UTC).isoformat(),
        "attempts": 0,
        "timeout_seconds": request.timeout_seconds,
        "idempotency_key": idempotency_key,
        "request_hash": request_hash,
    }
    TASKS[task_id]["request"] = request.model_dump()
    try:
        _persist_task(TASKS[task_id])
    except sqlite3.IntegrityError:
        # Two callers can race between _find_idempotent and the insert. The
        # unique SQLite index is the final arbiter; return its existing task
        # instead of leaking a 500 or submitting a duplicate generation.
        if idempotency_key:
            TASKS.pop(task_id, None)
            existing = _find_idempotent(idempotency_key, request_hash)
            if existing is not None:
                return _public_task(existing)
        raise
    QUEUE_WAKE.set()
    if request.wait:
        deadline = time.monotonic() + request.timeout_seconds + 15
        while True:
            latest = _load_task(task_id) or TASKS[task_id]
            if latest.get("status") not in {"queued", "running", "cancelling"}:
                break
            if time.monotonic() >= deadline:
                raise HTTPException(
                    status_code=504,
                    detail={
                        "message": "等待 Worker/生成结果超时，任务仍保留在持久化队列中",
                        "task_id": task_id,
                    },
                )
            await asyncio.sleep(0.5)
    return _public_task(_load_task(task_id) or TASKS[task_id])


@app.get("/api/generations")
async def generations() -> dict[str, Any]:
    _refresh_task_cache()
    items = sorted(_task_snapshots(), key=lambda item: item.get("created_at", ""), reverse=True)
    return {"tasks": [_public_task(item) for item in items[:100]], "count": len(items)}


class BatchGenerationRequest(BaseModel):
    requests: list[GenerationRequest] = Field(min_length=1, max_length=50)


@app.post("/api/generations/batch", status_code=202)
async def create_generation_batch(payload: BatchGenerationRequest) -> dict[str, Any]:
    tasks = []
    errors = []
    for index, item in enumerate(payload.requests):
        try:
            tasks.append(await create_generation(item))
        except HTTPException as exc:
            errors.append({"index": index, "status": exc.status_code, "detail": exc.detail})
    return {"tasks": tasks, "errors": errors, "count": len(tasks), "failed": len(errors)}


@app.get("/api/generations/{task_id}")
async def generation(task_id: str) -> dict[str, Any]:
    _refresh_task_cache()
    task = _load_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return _public_task(task)


@app.get("/api/generations/{task_id}/events")
async def generation_events(task_id: str) -> StreamingResponse:
    if _load_task(task_id) is None:
        raise HTTPException(status_code=404, detail="任务不存在")

    async def event_stream():
        last_payload = None
        deadline = time.monotonic() + GENERATION_TIMEOUT_SECONDS + 300
        while time.monotonic() < deadline:
            task = _load_task(task_id)
            if task is None:
                break
            payload = _public_task(task)
            encoded = json.dumps(payload, ensure_ascii=False, default=str)
            if encoded != last_payload:
                yield f"data: {encoded}\n\n"
                last_payload = encoded
            if task.get("status") in {"succeeded", "failed", "timed_out", "cancelled", "indeterminate", "interrupted"}:
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/api/generations/{task_id}/cancel")
async def cancel_generation(task_id: str) -> dict[str, Any]:
    task = _load_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if task.get("status") in {"succeeded", "failed", "timed_out", "cancelled", "indeterminate", "interrupted"}:
        raise HTTPException(status_code=409, detail="任务已经结束，不能取消")
    if task.get("status") == "queued":
        task["status"] = "cancelled"
        task["finished_at"] = datetime.now(UTC).isoformat()
        task["error"] = "任务在提交到 Flow 前已取消"
        _persist_task(task)
        QUEUE_WAKE.set()
        return _public_task(task)
    handle = TASK_HANDLES.get(task_id)
    if handle is None or handle.done():
        task["cancel_requested"] = True
        if task.get("status") == "running":
            task["status"] = "cancelling"
        _persist_task(task)
        return _public_task(task)
    task["status"] = "cancelling"
    _persist_task(task)
    handle.cancel()
    return _public_task(task)


@app.post("/api/generations/{task_id}/retry", status_code=202)
async def retry_generation(task_id: str) -> dict[str, Any]:
    original = _load_task(task_id)
    if original is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    if original.get("status") in {"queued", "running", "cancelling"}:
        raise HTTPException(status_code=409, detail="任务仍在运行或排队中，不能重复重试")
    if original.get("status") == "succeeded":
        raise HTTPException(status_code=409, detail="成功任务无需重试")
    request_data = original.get("request")
    if not isinstance(request_data, dict):
        raise HTTPException(status_code=409, detail="旧任务缺少可重试请求参数")
    retry_key = f"{original.get('idempotency_key')}:retry:{uuid.uuid4().hex}" if original.get("idempotency_key") else f"retry:{original['id']}:{uuid.uuid4().hex}"
    request = GenerationRequest.model_validate({**request_data, "idempotency_key": retry_key, "wait": False})
    return await create_generation(request)


@app.get("/api/media")
async def media(path: str) -> Response:
    """Serve only files under gflow output or studio upload roots."""
    if _configured_cloud_path(path):
        return StreamingResponse(_cloud_media_stream(path), media_type=mimetypes.guess_type(path)[0] or "application/octet-stream")
    return FileResponse(_resolve_media_path(path))


@app.get("/api/media/signed")
async def signed_media(
    exp: int,
    sig: str,
    token: str | None = Query(default=None),
    path: str | None = Query(default=None),
) -> Response:
    """Serve a time-limited result URL without exposing local paths or cookies."""
    if token:
        path = _path_from_media_token(token)
    if not path:
        raise HTTPException(status_code=403, detail="结果链接无效")
    if exp < int(time.time()) or not hmac.compare_digest(sig, _media_signature(path, exp)):
        raise HTTPException(status_code=403, detail="结果链接已过期或无效")
    if _configured_cloud_path(path):
        return StreamingResponse(_cloud_media_stream(path), media_type=mimetypes.guess_type(path)[0] or "application/octet-stream")
    return FileResponse(_resolve_media_path(path))


def _cleanup_uploads() -> int:
    cutoff = time.time() - ASSET_RETENTION_SECONDS
    removed = 0
    for candidate in UPLOAD_DIR.iterdir():
        try:
            if candidate.is_file() and candidate.stat().st_mtime < cutoff:
                candidate.unlink()
                removed += 1
        except OSError:
            continue
    try:
        with sqlite3.connect(DB_PATH) as conn:
            stale = conn.execute("SELECT local_path FROM studio_assets WHERE expires_at <= ?", (datetime.now(UTC).isoformat(),)).fetchall()
            conn.execute("DELETE FROM studio_assets WHERE expires_at <= ?", (datetime.now(UTC).isoformat(),))
            conn.commit()
        for (path,) in stale:
            Path(path).unlink(missing_ok=True)
    except sqlite3.Error:
        pass
    return removed


@app.post("/api/assets/cleanup")
async def cleanup_assets() -> dict[str, Any]:
    return {"removed": _cleanup_uploads(), "retention_seconds": ASSET_RETENTION_SECONDS}
