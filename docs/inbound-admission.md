# Pre-agent admission

Pass an `AdmissionHandler` as `ChannelManager(inbound_handler=...)`, or use
`manager.set_inbound_handler(handler)` to replace/clear it. Each accepted input
passes through the hook before session batching and model processing.

```python
async def admission(channel, message, dispatch):
    # Return True to claim input, False to retain native routing.
    return False

manager.set_inbound_handler(admission)
```

The hook may retain `dispatch` for a bounded delayed task. Dispatch holds the
channel/session lock and refuses removed or replaced channels and stopped
managers. `activate=True` only activates groups with group policy enabled and
visibility `all`; native sender checks and tool ceilings still apply. The host
must cancel its plugin tasks before stopping the manager or unloading plugins.

`InboundReceipt` records `attempted` and `delivered` separately. A send failure
after an attempt is not proof of non-delivery; callers must not blindly resend.
Cancellation propagates. Ordinary group input fails closed on hook errors;
native commands, direct messages and mentions retain normal routing. The default
without a hook preserves existing batching and routing behavior.

QQ adapters expose a copy of boolean `group_context` values through `plugin_group_settings`. Plugin-owned keys are preserved without adding plugin-specific fields to Gateway. A handler uses its declared default when its key is absent.

Delivery receipts track native `reply_*` sends in the current dispatch context; independent proactive pushes are not a receipt for this turn.
