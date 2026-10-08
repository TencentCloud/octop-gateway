# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 修复

- 元宝：群聊启用共享 @ 提及策略（`group_context` 默认 `enabled` + `activation=mention`），不再对每条群消息都回；解析 `TIMCustomElem`（`elem_type=1002`）写入 `bot_mentioned` / `mentioned_user_ids` / `at_elems` / `at_all`，并支持 `@所有人`。私聊无 @ 语义，不受影响。
- 元宝：群聊中机器人账号（含自己）的消息默认丢弃（`ignore_bot_senders`），避免同群两个机器人互答刷屏。
- `GroupContextManager.will_trigger()`：只读预判，通道据此跳过未触发消息的 typing 心跳。

## [1.0.0] - 2026-09-24

### 新增

- 首个 1.0.0 正式版本发布到 PyPI。

### 变更

- 对齐 Octop：引入 `develop` 集成分支策略；禁止直推 `main`/`develop`；发版后由 `sync-main-to-develop.yml` 同步；新增 `/publish` skill（发版同步 CHANGELOG / README）。


