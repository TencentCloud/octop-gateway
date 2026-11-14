"""Dynamic removal must drain its own activities before closing transport."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from octop_gateway.channel import BaseChannel
from octop_gateway.manager import ChannelManager
from octop_gateway.models import ChannelSubject, ContentPart, InboundMessage, MessageEvent, TextContent


async def _unused_processor(message: InboundMessage) -> AsyncIterator[MessageEvent]:
    yield MessageEvent.completed()


class LifecycleChannel(BaseChannel):
    """Controlled transport; manager queues, locks and tasks stay real."""

    channel_type = "lifecycle"

    def __init__(self, channel_id: str) -> None:
        super().__init__(_unused_processor, channel_id=channel_id)
        self.connected = False
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release_cleanup = asyncio.Event()
        self.cleaned = asyncio.Event()
        self.stopped = asyncio.Event()
        self.received = asyncio.Event()
        self.cleanup_connected: bool | None = None
        self.manager: ChannelManager | None = None
        self.raise_in_cleanup = False
        self.raise_in_stop = False

    async def start(self) -> None:
        self.connected = True

    async def stop(self) -> None:
        self.connected = False
        self.stopped.set()
        if self.raise_in_stop:
            raise RuntimeError("adapter stop failed")

    def parse_inbound(self, raw_payload: Any) -> InboundMessage:
        if isinstance(raw_payload, InboundMessage):
            return raw_payload
        return InboundMessage(
            channel_id=self.channel_id,
            channel_type=self.channel_type,
            channel_subject=ChannelSubject(subject_id="subject"),
            content=[TextContent(text=str(raw_payload))],
        )

    async def handle_inbound(self, raw_payload: Any) -> None:
        message = self.parse_inbound(raw_payload)
        if message.text == "ready":
            self.received.set()
            return
        self.entered.set()
        try:
            if message.text == "remove-self":
                assert self.manager is not None
                await self.manager.remove_channel(self.channel_id)
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
            try:
                await self.release_cleanup.wait()
            finally:
                self.cleanup_connected = self.connected
                self.cleaned.set()
            if self.raise_in_cleanup:
                raise RuntimeError("cleanup failed")

    async def _send_text(self, subject: ChannelSubject, text: str) -> None:
        pass

    async def _send_content(self, subject: ChannelSubject, parts: list[ContentPart]) -> None:
        pass

    async def _send_media(self, subject: ChannelSubject, media: ContentPart) -> None:
        pass


async def _entered(manager: ChannelManager, channel: LifecycleChannel, text: str = "hold") -> None:
    manager.enqueue(channel.channel_id, text)
    await asyncio.wait_for(channel.entered.wait(), timeout=2)


async def _release_all(
    manager: ChannelManager, channels: list[LifecycleChannel], tasks: list[asyncio.Task[Any]]
) -> None:
    for channel in channels:
        channel.release_cleanup.set()
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2)
    await asyncio.wait_for(manager.stop(), timeout=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("dynamic", [False, True])
async def test_remove_waits_for_worker_cleanup_before_transport_stop(dynamic: bool) -> None:
    channel = LifecycleChannel("worker")
    manager = ChannelManager({} if dynamic else {channel.channel_id: channel}, workers_per_channel=1)
    await manager.start()
    if dynamic:
        await manager.add_channel(channel)
    worker = manager._workers[0]
    await _entered(manager, channel)
    removal = asyncio.create_task(manager.remove_channel(channel.channel_id))
    try:
        await asyncio.wait_for(channel.cancelled.wait(), timeout=2)
        assert not channel.stopped.is_set(), "transport closed while worker cleanup was pending"
        assert not removal.done(), "remove returned before its worker finished"
        channel.release_cleanup.set()
        await asyncio.wait_for(removal, timeout=2)
        assert worker.done()
        assert channel.cleaned.is_set()
        assert channel.cleanup_connected is True
        assert channel.stopped.is_set()
    finally:
        await _release_all(manager, [channel], [removal, worker])


@pytest.mark.asyncio
async def test_remove_does_not_cancel_worker_for_prefix_channel_id() -> None:
    channel = LifecycleChannel("foo")
    other = LifecycleChannel("foo-bar")
    manager = ChannelManager({channel.channel_id: channel, other.channel_id: other}, workers_per_channel=1)
    await manager.start()
    workers = list(manager._workers)
    other_worker = workers[1]
    try:
        await manager.remove_channel(channel.channel_id)
        assert other_worker.cancelling() == 0, "removing foo cancelled foo-bar's worker"
        manager.enqueue(other.channel_id, "ready")
        await asyncio.wait_for(other.received.wait(), timeout=2)
        assert not other.stopped.is_set()
    finally:
        await _release_all(manager, [channel, other], workers)


@pytest.mark.asyncio
async def test_remove_cancels_and_drains_owned_interrupt_before_stop() -> None:
    channel = LifecycleChannel("interrupt")
    manager = ChannelManager(
        {channel.channel_id: channel},
        workers_per_channel=1,
        interrupt_message_predicate=lambda message: message.text == "interrupt",
    )
    await manager.start()
    workers = list(manager._workers)
    await _entered(manager, channel, "interrupt")
    interrupt = next(iter(manager._interrupt_tasks))
    removal = asyncio.create_task(manager.remove_channel(channel.channel_id))
    cancellation = asyncio.create_task(channel.cancelled.wait())
    try:
        await asyncio.wait_for(asyncio.wait([removal, cancellation], return_when=asyncio.FIRST_COMPLETED), timeout=2)
        assert channel.cancelled.is_set(), "removed channel's interrupt was left running"
        assert not channel.stopped.is_set()
        channel.release_cleanup.set()
        await asyncio.wait_for(removal, timeout=2)
        assert interrupt.done()
        assert channel.cleanup_connected is True
    finally:
        await _release_all(manager, [channel], [removal, cancellation, interrupt, *workers])


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_fails", [False, True])
async def test_cancelled_remove_still_stops_transport_and_propagates_cancellation(
    stop_fails: bool, caplog: pytest.LogCaptureFixture
) -> None:
    channel = LifecycleChannel("cancel-remove")
    channel.raise_in_stop = stop_fails
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=1)
    await manager.start()
    worker = manager._workers[0]
    await _entered(manager, channel)
    removal = asyncio.create_task(manager.remove_channel(channel.channel_id))
    try:
        await asyncio.wait_for(channel.cancelled.wait(), timeout=2)
        removal.cancel()
        with pytest.raises(asyncio.CancelledError):
            await removal
        assert worker.done()
        assert channel.cleaned.is_set()
        assert channel.cleanup_connected is True
        assert channel.stopped.is_set()
        if stop_fails:
            assert "adapter stop failed" in caplog.text
    finally:
        await _release_all(manager, [channel], [removal, worker])


@pytest.mark.asyncio
async def test_remove_propagates_adapter_stop_failure_without_caller_cancellation() -> None:
    channel = LifecycleChannel("stop-error")
    channel.raise_in_stop = True
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=1)
    await manager.start()
    workers = list(manager._workers)
    try:
        with pytest.raises(RuntimeError, match="adapter stop failed"):
            await manager.remove_channel(channel.channel_id)
        assert all(worker.done() for worker in workers)
        assert channel.stopped.is_set()
    finally:
        await _release_all(manager, [channel], workers)


@pytest.mark.asyncio
async def test_self_remove_keeps_cancellation_without_awaiting_own_task() -> None:
    channel = LifecycleChannel("self")
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=1)
    channel.manager = manager
    channel.release_cleanup.set()
    await manager.start()
    worker = manager._workers[0]
    try:
        await _entered(manager, channel, "remove-self")
        await asyncio.wait_for(channel.cleaned.wait(), timeout=2)
        await asyncio.wait_for(worker, timeout=2)
        assert channel.stopped.is_set()
        # The initiator cannot await its own finally before disconnecting.
        assert channel.cleanup_connected is False
    finally:
        await _release_all(manager, [channel], [worker])


@pytest.mark.asyncio
async def test_global_stop_drains_cleanup_before_stopping_transport() -> None:
    channel = LifecycleChannel("global")
    manager = ChannelManager({channel.channel_id: channel}, workers_per_channel=1)
    await manager.start()
    worker = manager._workers[0]
    await _entered(manager, channel)
    shutdown = asyncio.create_task(manager.stop())
    try:
        await asyncio.wait_for(channel.cancelled.wait(), timeout=2)
        assert not channel.stopped.is_set()
        channel.release_cleanup.set()
        await asyncio.wait_for(shutdown, timeout=2)
        assert worker.done()
        assert channel.cleanup_connected is True
        assert channel.stopped.is_set()
    finally:
        await _release_all(manager, [channel], [shutdown, worker])


@pytest.mark.asyncio
async def test_completed_interrupt_does_not_retain_task_ownership() -> None:
    channel = LifecycleChannel("completed-interrupt")
    manager = ChannelManager(
        {channel.channel_id: channel},
        workers_per_channel=1,
        interrupt_message_predicate=lambda message: message.text == "ready",
    )
    await manager.start()
    try:
        manager._put_to_queue(manager._queues[channel.channel_id], channel.channel_id, "ready")
        interrupt = next(iter(manager._interrupt_tasks))
        await asyncio.wait_for(interrupt, timeout=2)
        assert channel.received.is_set()
        assert interrupt not in manager._interrupt_tasks
        assert interrupt not in manager._task_channels
    finally:
        await _release_all(manager, [channel], list(manager._workers))
    assert not manager._task_channels


@pytest.mark.asyncio
async def test_interrupt_cleanup_failure_does_not_skip_transport_stop() -> None:
    channel = LifecycleChannel("failed-cleanup")
    channel.raise_in_cleanup = True
    manager = ChannelManager(
        {channel.channel_id: channel},
        workers_per_channel=1,
        interrupt_message_predicate=lambda message: message.text == "interrupt",
    )
    await manager.start()
    workers = list(manager._workers)
    await _entered(manager, channel, "interrupt")
    interrupt = next(iter(manager._interrupt_tasks))
    removal = asyncio.create_task(manager.remove_channel(channel.channel_id))
    try:
        await asyncio.wait_for(channel.cancelled.wait(), timeout=2)
        channel.release_cleanup.set()
        await asyncio.wait_for(removal, timeout=2)
        assert interrupt.done()
        assert channel.cleanup_connected is True
        assert channel.stopped.is_set()
    finally:
        await _release_all(manager, [channel], [removal, interrupt, *workers])
