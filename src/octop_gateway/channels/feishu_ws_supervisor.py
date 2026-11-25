"""Supervise a separate Feishu WebSocket worker for each application."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.request import getproxies

logger = logging.getLogger(__name__)

_STARTUP_TIMEOUT = 35.0
_HEARTBEAT_TIMEOUT = 15.0
_STOP_TIMEOUT = 5.0
_MAX_FRAME_SIZE = 4 * 1024 * 1024
_START_SLOTS = threading.BoundedSemaphore(3)
_OWNERS: set[str] = set()
_OWNERS_LOCK = threading.Lock()


def find_worker() -> Path | None:
    configured = os.environ.get("OCTOP_FEISHU_WS_WORKER")
    if configured:
        return Path(configured).expanduser().resolve(strict=True)
    name = "feishu-ws.exe" if sys.platform == "win32" else "feishu-ws"
    bundled = Path(__file__).resolve().parents[1] / "bin" / name
    return bundled if bundled.is_file() else None


def _worker_environment() -> dict[str, str]:
    # Go reads proxy environment variables; on Windows urllib also sees the
    # system proxy used by the Python SDK.
    environment = os.environ.copy()
    for scheme, proxy in getproxies().items():
        if scheme in ("http", "https", "no"):
            environment.setdefault(f"{scheme.upper()}_PROXY", proxy)
    return environment


class FeishuWSSupervisor:
    """Own one active process and pass its messages to the gateway loop."""

    def __init__(
        self,
        worker: Path,
        *,
        app_id: str,
        app_secret: str,
        verification_token: str,
        encrypt_key: str,
        domain: str | None,
        on_ready: Callable[[], None],
        on_message: Callable[[dict[str, Any]], Awaitable[bool]],
    ) -> None:
        self._worker = worker
        self._app_id = app_id
        self._config = {
            "app_id": app_id,
            "app_secret": app_secret,
            "verification_token": verification_token,
            "encrypt_key": encrypt_key,
            "domain": domain,
        }
        self._on_ready = on_ready
        self._on_message = on_message
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._state = "starting"
        self._pid: int | None = None
        self._generation = 0
        self._last_heartbeat_at: float | None = None
        self._last_error: str | None = None

    def health_metrics(self) -> dict[str, Any]:
        return {
            "feishu_ws_app_id": self._app_id,
            "feishu_ws_state": self._state,
            "feishu_ws_worker_pid": self._pid,
            "feishu_ws_generation": self._generation,
            "feishu_ws_last_heartbeat_at": self._last_heartbeat_at,
            "feishu_ws_last_error": self._last_error,
        }

    def start(self) -> None:
        with _OWNERS_LOCK:
            if self._app_id in _OWNERS:
                raise ValueError(f"Feishu application {self._app_id} already has a WebSocket worker")
            _OWNERS.add(self._app_id)
        self._task = asyncio.create_task(self._supervise(), name=f"feishu-ws-{self._app_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await asyncio.shield(self._task)

    async def _supervise(self) -> None:
        stopping = asyncio.create_task(self._stop.wait())
        failures = 0
        loop = asyncio.get_running_loop()
        try:
            while not self._stop.is_set():
                self._state = "starting"
                while not _START_SLOTS.acquire(blocking=False):
                    try:
                        await asyncio.wait_for(self._stop.wait(), random.uniform(0.10, 0.35))
                        return
                    except TimeoutError:
                        pass
                session_started = loop.time()
                try:
                    if not await self._run_worker(stopping):
                        return
                except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                    if not self._stop.is_set():
                        self._last_error = type(exc).__name__
                        logger.warning(
                            "Feishu worker failed: app_id=%s reason=%s",
                            self._app_id,
                            type(exc).__name__,
                        )
                if self._stop.is_set():
                    break
                self._state = "retrying"
                if loop.time() - session_started >= 60:
                    failures = 0
                delay = min(5 * 2**failures, 60)
                failures = min(failures + 1, 4)
                try:
                    await asyncio.wait_for(self._stop.wait(), random.uniform(delay / 2, delay))
                except TimeoutError:
                    continue
        finally:
            if self._stop.is_set():
                self._state = "stopped"
            stopping.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stopping
            with _OWNERS_LOCK:
                _OWNERS.remove(self._app_id)

    async def _run_worker(self, stopping: asyncio.Task[bool]) -> bool:
        process: asyncio.subprocess.Process | None = None
        starting = True
        try:
            process = await asyncio.create_subprocess_exec(
                str(self._worker),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=_MAX_FRAME_SIZE,
                env=_worker_environment(),
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            self._pid = process.pid
            self._generation += 1
            self._last_heartbeat_at = None
            logger.info("Feishu worker starting: app_id=%s pid=%s", self._app_id, process.pid)
            assert process.stdin is not None
            assert process.stdout is not None
            await self._write(process.stdin, self._config)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + _STARTUP_TIMEOUT
            while not self._stop.is_set():
                timeout = max(0.0, deadline - loop.time()) if starting else _HEARTBEAT_TIMEOUT
                line = await self._read(process.stdout, stopping, timeout)
                if line is None:
                    return True
                if not line:
                    raise RuntimeError("Feishu worker closed its output")
                frame = json.loads(line)
                if not isinstance(frame, dict):
                    raise ValueError("Invalid Feishu worker frame")
                kind = frame.get("kind")
                if kind == "ready" and starting:
                    _START_SLOTS.release()
                    starting = False
                    self._state = "connected"
                    self._last_error = None
                    self._on_ready()
                    logger.info("Feishu worker connected: app_id=%s pid=%s", self._app_id, process.pid)
                elif kind == "heartbeat":
                    self._last_heartbeat_at = time.time()
                elif kind == "rejected":
                    self._state = "rejected"
                    self._last_error = "credentials_or_configuration"
                    logger.error(
                        "Feishu rejected application %s; verify credentials and channel configuration", self._app_id
                    )
                    return False
                elif kind == "message":
                    frame_id = frame.get("id")
                    payload = frame.get("payload")
                    if not isinstance(frame_id, str) or not isinstance(payload, dict):
                        raise ValueError("Invalid Feishu worker message")
                    # The SDK can begin reading just before its ready callback.
                    try:
                        accepted = await asyncio.wait_for(self._on_message(payload), 1.5)
                    except TimeoutError:
                        logger.warning("Feishu message acceptance timed out: app_id=%s", self._app_id)
                        accepted = False
                    except Exception:
                        logger.exception("Feishu message acceptance failed: app_id=%s", self._app_id)
                        accepted = False
                    await self._write(process.stdin, {"id": frame_id, "accepted": accepted})
                else:
                    raise ValueError("Unknown Feishu worker frame")
            return True
        finally:
            if process is not None:
                try:
                    await self._shutdown(process)
                finally:
                    self._pid = None
                    if starting:
                        _START_SLOTS.release()
            elif starting:
                _START_SLOTS.release()

    @staticmethod
    async def _write(writer: asyncio.StreamWriter, frame: dict[str, Any]) -> None:
        writer.write(json.dumps(frame, ensure_ascii=False).encode("utf-8") + b"\n")
        await asyncio.wait_for(writer.drain(), 2.0)

    @staticmethod
    async def _read(reader: asyncio.StreamReader, stopping: asyncio.Task[bool], timeout: float) -> bytes | None:
        reading = asyncio.create_task(reader.readline())
        try:
            done, _ = await asyncio.wait((reading, stopping), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if reading in done:
                return reading.result()
            if stopping in done:
                return None
            raise TimeoutError("Feishu worker missed its deadline")
        finally:
            if not reading.done():
                reading.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await reading

    async def _shutdown(self, process: asyncio.subprocess.Process) -> None:
        if process.stdin is not None:
            process.stdin.close()
        # Drain while the worker exits so wait() can finish even with a full pipe.
        cleanup = asyncio.create_task(process.communicate())
        try:
            await asyncio.wait_for(asyncio.shield(cleanup), _STOP_TIMEOUT)
        except TimeoutError:
            logger.warning("Terminating unresponsive Feishu worker: app_id=%s pid=%s", self._app_id, process.pid)
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await cleanup
