"""Durable gflow studio queue worker.

Run this as a separate service from the FastAPI process. It imports the same
SQLite-backed control plane and delegates actual browser work to gflow-cli.
Only one worker instance should be active for a database because each Chrome
profile has one exclusive lease.
"""

from __future__ import annotations

import asyncio
import signal

import app as studio


async def main() -> None:
    studio.STUDIO_ROLE = "worker"
    studio._load_studio_config()
    studio.configure_rate_limiter(studio.GENERATION_RATE_CAPACITY, studio.GENERATION_RATE_REFILL_SECONDS)
    studio._load_persisted_tasks()
    studio._cleanup_uploads()
    studio.QUEUE_STOP = False
    studio._acquire_worker_lease()
    studio.WORKER_LEASE_HEARTBEAT = asyncio.create_task(studio._worker_lease_heartbeat())
    studio.QUEUE_DISPATCHER = asyncio.create_task(studio._queue_dispatcher())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError):
            pass
    try:
        await stop.wait()
    finally:
        await studio.stop_state()


if __name__ == "__main__":
    asyncio.run(main())
