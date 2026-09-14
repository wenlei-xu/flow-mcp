"""OpenAI-compatible HTTP facade for the local gflow daemon.

The adapter lives in the same process as ``gflow serve``.  It deliberately
calls the registered gflow generation functions directly instead of starting a
second browser or a second queue.  This keeps one warm Chrome profile and one
FlowWorker while giving browser-based clients a conventional ``/v1`` API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool
import uvicorn

from gflow_cli.config import get_settings

log = logging.getLogger(__name__)

_VIDEO_MODELS = (
    "omni_flash",
    "veo_3_1_lite",
    "veo_3_1_fast",
    "veo_3_1_quality",
    "veo_3_1_lite_lower_priority",
)
_IMAGE_MODELS = ("nano2", "nano-pro", "image4")
_MAX_UPLOAD_BYTES = 20 * 1024 * 1024


@dataclass
class _TaskRecord:
    id: str
    kind: str
    model: str
    status: str = "queued"
    progress: int = 0
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    files: list[str] = field(default_factory=list)
    error: str | None = None


def _data_dir() -> Path:
    configured = os.getenv("GFLOW_API_DATA_DIR", "").strip()
    root = Path(configured).expanduser() if configured else get_settings().output_dir / "api"
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def _task_dir(task_id: str) -> Path:
    path = _data_dir() / "tasks" / task_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _task_record_path(task_id: str) -> Path:
    path = _data_dir() / "records" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _save_task(task: _TaskRecord) -> None:
    task.updated = time.time()
    target = _task_record_path(task.id)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(task), ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)


def _load_task(task_id: str) -> _TaskRecord | None:
    path = _task_record_path(task_id)
    if not path.exists():
        return None
    try:
        return _TaskRecord(**json.loads(path.read_text(encoding="utf-8")))
    except (OSError, TypeError, ValueError):
        return None


def _require_api_key(request: Request) -> None:
    expected = os.getenv("GFLOW_API_KEY", "").strip()
    if not expected:
        return
    received = request.headers.get("authorization", "")
    if received != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Invalid API key")


def _public_url(request: Request, path: str) -> str:
    base = os.getenv("GFLOW_API_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if base:
        return f"{base}{path}"
    return f"{str(request.base_url).rstrip('/')}{path}"


def _error_message(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("detail", "message", "title", "error"):
            if value.get(key):
                return _error_message(value[key])
    return "gflow generation failed"


def _result_files(result: dict[str, Any]) -> list[str]:
    files = result.get("files")
    if isinstance(files, str):
        return [files]
    if isinstance(files, list):
        return [item for item in files if isinstance(item, str) and item]
    return []


def _output_files(output: Path, result_files: list[str]) -> list[str]:
    """Prefer the requested output path, including numbered batch siblings."""
    candidates = [output]
    for index in range(1, 5):
        candidates.append(output.parent / f"{output.stem}_{index}{output.suffix}")
    existing = [str(path) for path in candidates if path.is_file()]
    return existing or result_files


def _aspect(value: str | None) -> str:
    raw = (value or "9:16").strip().lower()
    if raw in {"9:16", "16:9", "1:1"}:
        return raw
    try:
        width, height = (int(item) for item in raw.split("x", 1))
    except (ValueError, TypeError):
        return "9:16"
    ratio = width / height if height else 0
    if ratio > 1.25:
        return "16:9"
    if ratio < 0.8:
        return "9:16"
    return "1:1"


def _video_model(value: str | None) -> str:
    raw = (value or "omni_flash").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "omni": "omni_flash",
        "flash": "omni_flash",
        "veo_lite": "veo_3_1_lite",
        "lite": "veo_3_1_lite",
        "veo_fast": "veo_3_1_fast",
        "fast": "veo_3_1_fast",
        "veo_quality": "veo_3_1_quality",
        "quality": "veo_3_1_quality",
        "veo_lite_lp": "veo_3_1_lite_lower_priority",
        "lower_priority": "veo_3_1_lite_lower_priority",
    }
    return aliases.get(raw, raw if raw in _VIDEO_MODELS else "omni_flash")


def _image_model(value: str | None) -> str:
    raw = (value or "nano2").strip().lower().replace("_", "-")
    if raw in {"nano2", "nano-pro", "image4"}:
        return raw
    return "nano2"


async def _save_upload(upload: UploadFile, target: Path) -> None:
    content = await upload.read()
    if len(content) > _MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Reference image exceeds the 20 MB limit")
    if not content:
        raise HTTPException(status_code=400, detail="Reference image is empty")
    target.parent.mkdir(parents=True, exist_ok=True)
    await run_in_threadpool(target.write_bytes, content)


async def _run_video_task(task: _TaskRecord, arguments: dict[str, Any], output: Path) -> None:
    task.status = "in_progress"
    task.progress = 1
    _save_task(task)
    try:
        from gflow_cli.mcp.server import no_spend_active
        from gflow_cli.mcp.tools import gflow_generate_video

        if no_spend_active():
            raise RuntimeError(
                "Video generation is disabled because gflow is running in no-spend mode"
            )

        result = await gflow_generate_video(**arguments, wait=True, output=str(output))
        if result.get("status") not in {"completed", "succeeded"}:
            raise RuntimeError(_error_message(result.get("error") or result))
        files = _output_files(output, _result_files(result))
        if not files:
            raise RuntimeError("gflow completed without returning a video file")
        task.files = files
        task.status = "completed"
        task.progress = 100
    except Exception as exc:  # noqa: BLE001 - API boundary converts failures to task state
        log.exception("gflow REST video task failed", exc_info=exc)
        task.status = "failed"
        task.error = str(exc)
    finally:
        shutil.rmtree(_task_dir(task.id) / "inputs", ignore_errors=True)
        _save_task(task)


async def _call_image(arguments: dict[str, Any], output: Path) -> list[str]:
    from gflow_cli.mcp.server import no_spend_active
    from gflow_cli.mcp.tools import gflow_generate_image

    if no_spend_active():
        raise HTTPException(status_code=403, detail="Image generation is disabled by no-spend mode")
    result = await gflow_generate_image(**arguments, wait=True, output=str(output))
    if result.get("status") not in {"completed", "succeeded"}:
        raise HTTPException(status_code=502, detail=_error_message(result.get("error") or result))
    files = _output_files(output, _result_files(result))
    if not files:
        raise HTTPException(
            status_code=502,
            detail="gflow completed without returning an image file",
        )
    return files


def create_app() -> FastAPI:
    app = FastAPI(title="gflow OpenAI API", version="1.0")
    origins = [
        item.strip()
        for item in os.getenv("GFLOW_API_CORS_ORIGINS", "").split(",")
        if item.strip()
    ]
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type"],
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "gflow-openai-api"}

    @app.get("/v1/models", dependencies=[Depends(_require_api_key)])
    async def models() -> dict[str, Any]:
        data = [
            *(
                {"id": model, "object": "model", "owned_by": "gflow", "capability": "video"}
                for model in _VIDEO_MODELS
            ),
            *(
                {"id": model, "object": "model", "owned_by": "gflow", "capability": "image"}
                for model in _IMAGE_MODELS
            ),
        ]
        return {"object": "list", "data": data}

    @app.post("/v1/videos", dependencies=[Depends(_require_api_key)])
    async def create_video(
        request: Request,
        prompt: str = Form(...),
        model: str = Form("omni_flash"),
        seconds: int | None = Form(None),
        size: str = Form("9:16"),
        mode: str = Form("frames"),
        profile: str = Form("default"),
        project: str | None = Form(None),
        ui_mode: str | None = Form(None),
        first_frame: UploadFile | None = File(None),
        last_frame: UploadFile | None = File(None),
        images: list[UploadFile] | None = File(None, alias="image[]"),
        single_image: UploadFile | None = File(None, alias="image"),
    ) -> JSONResponse:
        from gflow_cli.mcp.server import no_spend_active

        if no_spend_active():
            raise HTTPException(
                status_code=403,
                detail="Video generation is disabled because gflow is running in no-spend mode",
            )
        task_id = str(uuid.uuid4())
        task = _TaskRecord(id=task_id, kind="video", model=_video_model(model))
        _save_task(task)
        input_dir = _task_dir(task_id) / "inputs"
        image_files: list[str] = []
        try:
            uploads = list(images or [])
            if single_image is not None:
                uploads.append(single_image)
            if first_frame is not None:
                first_path = input_dir / "first.png"
                await _save_upload(first_frame, first_path)
            else:
                first_path = None
            if last_frame is not None:
                last_path = input_dir / "last.png"
                await _save_upload(last_frame, last_path)
            else:
                last_path = None
            for index, upload in enumerate(uploads):
                suffix = Path(upload.filename or ".png").suffix or ".png"
                path = input_dir / f"reference_{index + 1}{suffix}"
                await _save_upload(upload, path)
                image_files.append(str(path))
            if first_path is None and image_files and mode != "reference":
                first_path = Path(image_files[0])
            if last_path is None and len(image_files) > 1 and mode != "reference":
                last_path = Path(image_files[1])

            if mode == "reference":
                gflow_mode = "r2v"
                refs = image_files
                initial_frame = None
                end_frame = None
            elif first_path is not None:
                gflow_mode = "i2v"
                refs = None
                initial_frame = str(first_path)
                end_frame = str(last_path) if last_path else None
            else:
                gflow_mode = "t2v"
                refs = None
                initial_frame = None
                end_frame = None

            output = _task_dir(task_id) / "result.mp4"
            arguments = {
                "prompt": prompt,
                "mode": gflow_mode,
                "aspect": _aspect(size),
                "model": _video_model(model),
                "duration": seconds,
                "count": 1,
                "profile": profile,
                "project": project or os.getenv("GFLOW_API_PROJECT") or None,
                "ui_mode": ui_mode,
                "initial_frame": initial_frame,
                "end_frame": end_frame,
                "reference_images": refs,
            }
            asyncio.create_task(_run_video_task(task, arguments, output))
        except Exception:
            shutil.rmtree(input_dir, ignore_errors=True)
            raise
        return JSONResponse(
            status_code=202,
            content={
                "id": task_id,
                "object": "video",
                "status": "queued",
                "model": task.model,
                "progress": 0,
                "url": _public_url(request, f"/v1/videos/{task_id}/content"),
            },
        )

    @app.get("/v1/videos/{task_id}", dependencies=[Depends(_require_api_key)])
    async def video_status(task_id: str) -> dict[str, Any]:
        task = _load_task(task_id)
        if task is None or task.kind != "video":
            raise HTTPException(status_code=404, detail="Video task not found")
        status = {
            "queued": "queued",
            "in_progress": "in_progress",
            "completed": "completed",
        }.get(task.status, "failed")
        response: dict[str, Any] = {
            "id": task.id,
            "object": "video",
            "status": status,
            "progress": task.progress,
            "model": task.model,
        }
        if task.status == "completed":
            response["url"] = f"/v1/videos/{task.id}/content"
        if task.error:
            response["error"] = {"message": task.error}
        return response

    @app.get("/v1/videos/{task_id}/content", dependencies=[Depends(_require_api_key)])
    async def video_content(task_id: str) -> FileResponse:
        task = _load_task(task_id)
        if task is None or task.kind != "video" or task.status != "completed" or not task.files:
            raise HTTPException(status_code=404, detail="Video content is not ready")
        path = Path(task.files[0]).resolve()
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Video file is no longer available")
        return FileResponse(path, media_type="video/mp4", filename=f"{task.id}.mp4")

    @app.post("/v1/images/generations", dependencies=[Depends(_require_api_key)])
    async def generate_image(request: Request) -> dict[str, Any]:
        body = await request.json()
        task_id = str(uuid.uuid4())
        output = _task_dir(task_id) / "result.png"
        files = await _call_image(
            {
                "prompt": str(body.get("prompt", "")),
                "model": _image_model(body.get("model")),
                "aspect": _aspect(body.get("size")),
                "count": max(1, min(4, int(body.get("n", 1)))),
                "profile": "default",
            },
            output,
        )
        record = _TaskRecord(
            id=task_id,
            kind="image",
            model=_image_model(body.get("model")),
            status="completed",
            progress=100,
            files=files,
        )
        _save_task(record)
        return {
            "created": int(time.time()),
            "data": [
                {"url": _public_url(request, f"/v1/media/{task_id}/{index}")}
                for index in range(len(files))
            ],
        }

    @app.post("/v1/images/edits", dependencies=[Depends(_require_api_key)])
    async def edit_image(
        request: Request,
        prompt: str = Form(...),
        model: str = Form("nano2"),
        image: list[UploadFile] | None = File(None),
        image_array: list[UploadFile] | None = File(None, alias="image[]"),
    ) -> dict[str, Any]:
        task_id = str(uuid.uuid4())
        input_dir = _task_dir(task_id) / "inputs"
        files = list(image or []) + list(image_array or [])
        references: list[str] = []
        for index, upload in enumerate(files):
            suffix = Path(upload.filename or ".png").suffix or ".png"
            path = input_dir / f"reference_{index + 1}{suffix}"
            await _save_upload(upload, path)
            references.append(str(path))
        if not references:
            raise HTTPException(status_code=400, detail="At least one reference image is required")
        output = _task_dir(task_id) / "result.png"
        try:
            generated = await _call_image(
                {
                    "prompt": prompt,
                    "model": _image_model(model),
                    "aspect": "1:1",
                    "count": 1,
                    "reference_images": references,
                    "profile": "default",
                },
                output,
            )
        finally:
            shutil.rmtree(input_dir, ignore_errors=True)
        record = _TaskRecord(
            id=task_id,
            kind="image",
            model=_image_model(model),
            status="completed",
            progress=100,
            files=generated,
        )
        _save_task(record)
        return {
            "created": int(time.time()),
            "data": [
                {"url": _public_url(request, f"/v1/media/{task_id}/{index}")}
                for index in range(len(generated))
            ],
        }

    # Image URLs are loaded by <img> / the browser's fetch() without custom
    # Authorization headers.  The identifier is an opaque UUID and the whole
    # host can still be protected by Cloudflare Access; keeping this one
    # response route header-free makes it compatible with existing clients.
    @app.get("/v1/media/{task_id}/{index}")
    async def media(task_id: str, index: int) -> FileResponse:
        task = _load_task(task_id)
        if task is None or task.kind != "image" or index < 0 or index >= len(task.files):
            raise HTTPException(status_code=404, detail="Media not found")
        path = Path(task.files[index]).resolve()
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Media file is no longer available")
        return FileResponse(path, media_type="image/png", filename=f"{task.id}-{index + 1}.png")

    return app


async def run_api(host: str, port: int) -> None:
    """Serve the REST facade in the same event loop as the MCP daemon."""
    config = uvicorn.Config(create_app(), host=host, port=port, log_config=None)
    await uvicorn.Server(config).serve()
