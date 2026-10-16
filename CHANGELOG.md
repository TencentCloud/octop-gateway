# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 新增

- 提供带插件所有权的动态通道注册、更新和注销接口。

## [1.0.2] - 2026-10-14

### 修复

- 微信思考状态改由 `sendtyping` 控制，正文发出前关闭，不再把「思考中」插到答案后面。思考灯失败且超过约 4 秒还没有正文时，才先发一条「⏳ 思考中...」。要灯和关灯的请求最多等 3 秒，避免卡住正文。
- 微信普通回复和主动推送超过约 2000 字时按换行切开再发，避免客户端显示不全。发送失败只记录返回码和字数，不记录发送票和正文。
- QQ 私聊不再把未完成的中间帧刷成空白气泡，整段回复落在同一个气泡里。

## [1.0.1] - 2026-10-09

### 新增

- Telegram：新增 `allowed_user_ids` 用户白名单（私聊与群聊均生效）。留空保持现有行为（任何人都能使用 Bot）；配置后只有列表中的 Telegram 用户 ID 能触发 Agent，其他消息在拉取媒体、调用 Agent 之前即被丢弃。

## [1.0.0] - 2026-09-24

### 新增

- 首个 1.0.0 正式版本发布到 PyPI。

### 变更

- 对齐 Octop：引入 `develop` 集成分支策略；禁止直推 `main`/`develop`；发版后由 `sync-main-to-develop.yml` 同步；新增 `/publish` skill（发版同步 CHANGELOG / README）。


