# Reliable Feishu reception

Feishu may deliver the same event more than once. Reconnecting a channel or
restarting the process must not create a new processor turn for a receipt that
has already completed. Event age is **not** proof of previous processing: a
message first delivered after an outage still needs to be accepted.

## Default behavior

Use `ChannelManager` for reliable Feishu channels. `FeishuConfig.inbox_path=None`
(the default) enables `~/.octop-gateway/feishu-inbox.sqlite3` when registered with
a manager, while preserving legacy standalone channel startup. In containers, mount that directory on
persistent storage and give the service user write permission. A read-only or
unavailable inbox causes reception to fail rather than acknowledge lost work.
The inbox contains message content; protect and back it up accordingly.

The SDK callback commits the normalized event before returning. Only then can
the SDK acknowledge successful reception. The manager polls pending receipts;
an in-memory queue is only a best-effort hint for urgent commands. No model,
media download or reply send runs inside the acceptance transaction.

Receipts are unique by application scope and message ID. The scope is
`feishu:{app_id}:im.message.receive_v1`, independent of connection session IDs
and generated channel instance IDs. Different apps can process the same group
message independently. Configure only one active consumer for each application
and inbox. The manager rejects duplicate application scopes in its channel
registry. Sharing an inbox across multiple active managers/processes for the
same application is not supported; the SQLite unique constraint protects
acceptance, but this release does not implement distributed consumer ownership.

Keep a given application's processor/tenant routing stable while it has pending
receipts. Changing the target agent is a host migration: reconcile existing
receipts first, rather than executing them against an unrelated processor.

```python
config = FeishuConfig(
    app_id="cli_example",
    app_secret="...",
    inbox_path="/var/lib/my-bot/feishu-inbox.sqlite3",
)
await manager.add_feishu_channel(config)
```

Default standalone `FeishuChannel.start()` keeps the existing best-effort path.
A host that intentionally accepts best-effort delivery under a manager can set
`inbox_path=""` to explicitly disable persistence. That mode uses instance-local bounded deduplication and has
**no restart-safe guarantee**. Do not use it to resolve redelivery after restart.
A nonempty `inbox_path` explicitly requests durable mode and requires a consumer;
a standalone channel without a consumer then fails startup rather than silently
accumulating unprocessed work. Before opening a transport, the manager performs
a storage write (`recover`) to validate schema and write availability.

## States and recovery

- `pending`: saved, not yet claimed; automatically consumed after restart.
- `waiting`: atomically reserved by a worker, but not yet in the processor;
  shutdown/startup returns this work to pending.
- `processing`: the session is available and processing is about to begin. Same-session work remains
  serialized; different sessions can progress independently.
- `completed`: the processor emitted COMPLETED and its channel delivery path
  returned successfully.
- `ignored`: group policy intentionally did not activate the processor.
- `failed`: processing failed, including errors caught by the shared channel
  pipeline. A normal return alone is not treated as success.
- `recovery_required`: a previous worker was interrupted. Startup moves old
  `processing` receipts here, rather than rerunning tools or replies whose
  effects may already have happened.

Pending receipts are not automatically batched together. Every accepted
message retains its own identity and result. Urgent control messages use the
manager's existing interrupt predicate and group authorization before bypassing
the session lock. Processor cancellation leaves work for reconciliation.

Hosts can inject an `InboxStore` by assigning `channel.inbound_store` and its
stable `channel.inbound_scope` before registration. The store methods are
synchronous: acceptance runs on the SDK thread and manager operations run in
`asyncio.to_thread`, never doing synchronous SQLite I/O on the main event loop.
Implementations must commit before returning `accept`, provide atomic
accept/claim, and bound acceptance I/O within Feishu's callback deadline.

To reconcile the built-in SQLite store:

```python
store = channel.inbound_store
scope = channel.inbound_scope
interrupted = await asyncio.to_thread(store.entries, scope, "recovery_required")
# Reconcile each receipt with your history/checkpoints/tool and delivery logs.
# Only retry when the task's side effects are known to be safe to repeat:
await asyncio.to_thread(store.resolve, scope, message_id, retry=True)
# Or mark a receipt complete after confirming it already finished:
await asyncio.to_thread(store.resolve, scope, message_id, retry=False)
```

Never automatically resolve all interrupted work for retry. A host task ID and
checkpoint are needed to resume a partially executed agent turn safely. The
processor receives `metadata.inbound_scope` and `metadata.inbound_message_id`
for stable correlation; Feishu also supplies `metadata.event_id`.

This is reliable acceptance and duplicate suppression, **not end-to-end exactly
once execution**. If a send succeeded remotely but the process crashed before
recording completion, the receipt needs reconciliation. Hosts needing automatic
reply recovery should persist final output in an outbox, use stable platform
idempotency keys where supported, and track unknown delivery outcomes. No
automatic whole-turn retry is performed here.

## Retention and migration

The default store does not silently evict IDs at a fixed cache size. Schedule
`store.prune(before_timestamp)` only after a deliberate retention window;
retain terminal dedup keys for at least seven days to cover cumulative retries
and operational delays. `prune` never deletes pending, processing, failed or
recovery-required receipts. After pruning, a much later duplicate can be treated
as new: retention is an explicit operational guarantee, not infinite storage.

The inbox initially has no evidence of messages processed by older releases.
Before reconnecting production bots, reconcile known historical message IDs
from host history/logs. For receipts known to have completed, accept their
original scope/ID, claim them and finish them as completed **before starting the
manager**. A `received` log alone is not completion evidence. Unknown historical
work needs reconciliation, not an age-based discard rule.

## Connection handling

Each Feishu client uses a private SDK module namespace, so lark-oapi's
module-level `loop` binding belongs to that client throughout start, receive,
ping, reconnect and shutdown. The process-wide SDK module is left unchanged.
This compatibility adapter loads the installed SDK implementation, not a copied
fork; its tests verify separate method globals and concurrent loop ownership.

Successful control-frame processing refreshes transport activity. Healthy
PONGs keep the watchdog alive even when nobody sends business messages.
`reaction.created_v1` is explicitly consumed without starting a processor turn;
other unknown events are not indiscriminately acknowledged.

Display-setting hot updates, agent checkpoints, outbound outbox storage and
client-visible time labels belong to the host application and are outside this
library's receipt store.
