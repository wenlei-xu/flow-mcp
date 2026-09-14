import importlib
import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gflow-cli" / "src"))
studio = importlib.import_module("app")


class StudioSmokeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_db = studio.DB_PATH
        self.old_upload = studio.UPLOAD_DIR
        studio.DB_PATH = Path(self.tmp.name) / "studio.db"
        studio.UPLOAD_DIR = Path(self.tmp.name) / "uploads"
        studio.UPLOAD_DIR.mkdir()
        studio.TASKS.clear()
        studio.TASK_HANDLES.clear()

    def tearDown(self):
        studio.DB_PATH = self.old_db
        studio.UPLOAD_DIR = self.old_upload
        studio.TASKS.clear()
        studio.TASK_HANDLES.clear()
        try:
            self.tmp.cleanup()
        except PermissionError:
            # Windows may hold SQLite's journal briefly after an async test
            # loop closes; the OS reclaims this temporary directory later.
            pass

    def test_signed_media_url_is_time_limited(self):
        asset = studio.UPLOAD_DIR / "asset.png"
        asset.write_bytes(b"png")
        with patch.object(studio, "_is_loopback", return_value=True), TestClient(studio.app) as client:
            response = client.get(studio._media_url(str(asset)))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"png")

    def test_uploaded_asset_returns_id_without_server_path(self):
        with patch.object(studio, "_is_loopback", return_value=True), TestClient(studio.app) as client:
            response = client.post(
                "/api/assets",
                files={"file": ("reference.png", b"fake-png", "image/png")},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["asset_id"])
        self.assertNotIn("path", payload)
        self.assertIn("preview_url", payload)

    def test_mcp_materialized_path_becomes_signed_url(self):
        asset = studio.UPLOAD_DIR / "mcp-result.png"
        asset.write_bytes(b"png")
        raw = json.dumps({"result": {"files": [str(asset)]}}).encode()
        rewritten = studio._rewrite_mcp_payload(raw, "application/json")
        payload = json.loads(rewritten)
        self.assertNotIn(str(asset), rewritten.decode())
        self.assertIn("/api/media/signed?", payload["result"]["files"][0])

    def test_queue_idempotency_and_batch(self):
        meta = studio.profile_store.ProfileMeta("qa", Path("."), True, datetime.now(UTC), True, "qa@example.com")

        async def generate(**kwargs):
            return {"status": "succeeded", "files": []}

        async def resolve(profile, requested):
            return "project-1"

        with patch.object(studio.profile_store, "list_profiles", return_value=[meta]), \
             patch.object(studio, "_is_loopback", return_value=True), \
             patch.object(studio, "gflow_generate_image", side_effect=generate), \
             patch.object(studio, "_resolve_project", side_effect=resolve), \
             TestClient(studio.app) as client:
            payload = {"kind": "image", "prompt": "test", "idempotency_key": "same-key", "wait": True}
            first = client.post("/api/generations", json=payload).json()
            second = client.post("/api/generations", json=payload).json()
            studio.TASKS.clear()
            after_cache_restart = client.post("/api/generations", json=payload).json()
            batch = client.post("/api/generations/batch", json={"requests": [{"kind": "image", "prompt": "a", "wait": True}, {"kind": "image", "prompt": "b", "wait": True}]})
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["id"], after_cache_restart["id"])
        self.assertEqual(batch.status_code, 202)
        self.assertEqual(batch.json()["count"], 2)

    def test_legacy_auth_probe_failure_does_not_remove_cookie_profile_from_pool(self):
        meta = studio.profile_store.ProfileMeta(
            "legacy", Path("."), True, datetime.now(UTC), True, "legacy@example.com"
        )
        studio._record_profile_health(
            "legacy",
            {"status": "no_session", "error": {"status": 401, "message": "old verifier"}},
        )

        with patch.object(studio.profile_store, "list_profiles", return_value=[meta]):
            pool = studio._pool_profiles()

        self.assertEqual([item.name for item in pool], ["legacy"])

    def test_separate_worker_discovers_and_atomically_claims_api_task(self):
        task = {
            "id": "durable-task",
            "request": {"kind": "image", "prompt": "queued"},
            "kind": "image",
            "status": "queued",
            "attempts": 0,
            "created_at": datetime.now(UTC).isoformat(),
            "available_at": datetime.now(UTC).isoformat(),
            "timeout_seconds": 1800,
        }
        studio._persist_task(task)
        studio.TASKS.clear()

        # This simulates the API process having written the row after the
        # Worker process started: the Worker must discover it from SQLite.
        studio._refresh_task_cache(all_queue_tasks=True)
        self.assertIn("durable-task", studio.TASKS)
        claimed = studio._claim_task("durable-task")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["status"], "running")
        self.assertEqual(claimed["attempts"], 1)
        self.assertIsNone(studio._claim_task("durable-task"))

        with studio.DB_PATH.open("rb") as handle:
            self.assertTrue(handle.read(16).startswith(b"SQLite format 3"))

    def test_old_persisted_task_is_queryable_after_cache_eviction(self):
        task = {
            "id": "old-task",
            "request": {"kind": "image", "prompt": "history"},
            "kind": "image",
            "status": "succeeded",
            "progress": 100,
            "phase": "已完成",
            "profile": "qa",
            "created_at": datetime.now(UTC).isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "attempts": 1,
            "timeout_seconds": 1800,
            "result": {"status": "succeeded", "files": []},
        }
        studio._persist_task(task)
        studio.TASKS.clear()

        with patch.object(studio, "_is_loopback", return_value=True), TestClient(studio.app) as client:
            generation = client.get("/api/generations/old-task")
            detail = client.get("/api/logs/old-task")
        self.assertEqual(generation.status_code, 200)
        self.assertEqual(generation.json()["id"], "old-task")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["log"]["id"], "old-task")

    def test_worker_lease_rejects_second_active_worker(self):
        studio._acquire_worker_lease("worker-one")
        try:
            with self.assertRaises(RuntimeError):
                studio._acquire_worker_lease("worker-two")
        finally:
            studio._release_worker_lease("worker-one")

    def test_profile_capacity_is_explicitly_single_session(self):
        studio._init_db()
        studio._upsert_profile_setting("safe", image_concurrency=1, video_concurrency=1)
        setting = studio._profile_setting("safe")
        self.assertEqual(setting["image_concurrency"], 1)
        self.assertEqual(setting["video_concurrency"], 1)

        with self.assertRaises(Exception):
            studio.ProfileCreateRequest.model_validate(
                {"name": "unsafe", "image_concurrency": 2, "video_concurrency": 1}
            )

    def test_openai_image_edits_persists_multipart_before_queueing(self):
        captured = {}

        async def fake_create_generation(request):
            captured["request"] = request
            return {"status": "succeeded", "result": {"files": []}}

        with patch.object(studio, "_is_loopback", return_value=True), \
             patch.object(studio, "create_generation", side_effect=fake_create_generation), \
             TestClient(studio.app) as client:
            response = client.post(
                "/v1/images/edits",
                data={"prompt": "edit", "n": "1", "size": "1024x1024"},
                files=[("image", ("reference.png", b"fake-png", "image/png"))],
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["request"].kind, "image")
        self.assertEqual(len(captured["request"].input_asset_ids), 1)

    def test_openai_responses_facade_preserves_async_task_shape(self):
        captured = {}

        async def fake_create_generation(request):
            captured["request"] = request
            return {
                "id": "response-task",
                "status": "succeeded",
                "error": "",
                "result": {"status": "succeeded", "files": []},
            }

        with patch.object(studio, "_is_loopback", return_value=True), \
             patch.object(studio, "create_generation", side_effect=fake_create_generation), \
             TestClient(studio.app) as client:
            response = client.post(
                "/v1/responses",
                json={"model": "nano-pro", "input": "draw a toy"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["request"].kind, "image")
        self.assertEqual(response.json()["object"], "response")
        self.assertEqual(response.json()["gflow_task_id"], "response-task")


if __name__ == "__main__":
    unittest.main()
