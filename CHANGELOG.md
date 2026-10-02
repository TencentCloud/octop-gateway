# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 修复

- 飞书通道 WS 线程生命周期可靠性（#1392）：
  - 停止 WS 线程前先等待线程就绪（有界），修复"取消请求在线程起跑前发出、
    根本没有被投递、线程被孤儿化并永久占用连接槽位"的竞态——连接检测按钮
    在生产环境两次触发该事故；
  - watchdog 面对孤儿线程不再无限推迟重启：超过 300 秒即强制重建（宁可冒
    EXCEED_CONN_LIMIT 风险，也不让通道永久静默）；
  - lark-oapi 全局事件循环支持"所有权跟踪"：从已停止的上一代实例手中收回
    全局循环，保证重启路径可自愈；对存活通道的保护（#757）保持不变。

## [1.0.0] - 2026-09-24

### 新增

- 首个 1.0.0 正式版本发布到 PyPI。

### 变更

- 对齐 Octop：引入 `develop` 集成分支策略；禁止直推 `main`/`develop`；发版后由 `sync-main-to-develop.yml` 同步；新增 `/publish` skill（发版同步 CHANGELOG / README）。


