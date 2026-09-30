# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 修复

- 修复微信通道「单条消息回复丢失」：`_process_inbound` 错误兜底路径中的补发（flush delta 与错误提示）改用 `_safe_send`，发送失败只记日志不再抛出——原先兜底复用已失效的出站链路，二次异常从 except 块逃逸并中断该条消息的全部后续投递（上游 `ret=-2` 抖动时约 20% 概率丢回复）。
- 微信发送对 `ret=-2/-14` 的重试移除 `context_token` 非空前置条件：无 context 的主动推送/工具提示现在也会重试一次，并加入 1 秒退避（与轮询退避策略精神一致），瞬时 `prepare failed` 可自愈；持续失败仍在一次重试后如实上抛。

## [1.0.0] - 2026-09-24

### 新增

- 首个 1.0.0 正式版本发布到 PyPI。

### 变更

- 对齐 Octop：引入 `develop` 集成分支策略；禁止直推 `main`/`develop`；发版后由 `sync-main-to-develop.yml` 同步；新增 `/publish` skill（发版同步 CHANGELOG / README）。


