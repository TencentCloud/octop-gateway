# QQ group metadata

`await qq_channel.get_group_info(group_openid)` returns the official QQ response,
including `group_name`, but not the numeric QQ group number. Only platform-
allowlisted bots may call this API; QQ code `11253` means permission is missing.
The channel enforces a separate 30-request/minute sliding window including
token retries. A caller sharing one bot across several channel instances must
coordinate that bot's combined quota. Waiting for a quota slot is cancellable;
UI callers should apply a timeout and cache metadata rather than query on every
message. The lookup does not change message receiving or sending behavior.

Official reference: https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_groups_group_openid_info.get.html
