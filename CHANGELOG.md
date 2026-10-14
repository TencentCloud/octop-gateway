# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 修复

- 修复微信通道「单条消息回复丢失」：`_process_inbound` 错误兜底路径中的补发（flush delta 与错误提示）改用 `_safe_send`，发送失败只记日志不再抛出——原先兜底复用已失效的出站链路，二次异常从 except 块逃逸并中断该条消息的全部后续投递（上游 `ret=-2` 抖动时会丢回复）。
- 微信发送对 `ret=-2/-14` 的重试移除 `context_token` 非空前置条件：无 context 的主动推送 / 工具提示现在也会重试一次，并加入 1 秒退避（与轮询退避策略一致），瞬时 `prepare failed` 可自愈；持续失败仍在一次重试后如实上抛。

## [1.0.2] - 2026-10-14

### 修复

- 微信思考状态改由 `sendtyping` 控制，正文发出前关闭，不再把「思考中」插到答案后面。思考灯失败且超过约 4 秒还没有正文时，才先发一条「⏳ 思考中...」。要灯和关灯的请求最多等 3 秒，避免卡住正文。
- 微信普通回复和主动推送超过约 2000 字时按换行切开再发，避免客户端显示不全。发送失败只记录返回码和字数，不记录发送票和正文。

## [1.0.1] - 2026-10-09

### 新增

- Telegram：新增 `allowed_user_ids` 用户白名单（私聊与群聊均生效）。留空保持现有行为（任何人都能使用 Bot）；配置后只有列表中的 Telegram 用户 ID 能触发 Agent，其他消息在拉取媒体、调用 Agent 之前即被丢弃。

## [1.0.0] - 2026-09-24

### 新增

- 首个 1.0.0 正式版本发布到 PyPI。

### 变更

- 对齐 Octop：引入 `develop` 集成分支策略；禁止直推 `main`/`develop`；发版后由 `sync-main-to-develop.yml` 同步；新增 `/publish` skill（发版同步 CHANGELOG / README）。


