# DBX Kafka

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[English](README.en.md) · [独立仓库迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md) · [Kafka MCP 参考](docs/MCP.zh-CN.md)

DBX Kafka 是面向 Apache Kafka 运维和排障的可视化工作台。它把 topic、消息、
消费组、ACL 和 Schema Registry 集中到一个界面，适合开发、测试和生产环境的
日常检查与受控操作。

## 适合场景

- 快速确认 topic、分区、消费组和集群元数据是否符合预期。
- 排查消息格式、过滤条件、消费延迟和 offset 问题。
- 在只读和确认策略保护下完成消息生产、offset 调整和 ACL 运维。

## 核心能力

- 浏览 topic、分区、ISR、配置和集群元数据。
- 生产和消费消息，支持过滤、字段检索、offset 策略、Base64 和常见压缩/解码。
- 流式消费支持暂停、恢复、缓冲和结果导出。
- 查看消费组 lag、成员和 offset，并支持受控的 offset 重置。
- 管理 topic、ACL 和相关配置，写操作受只读和删除确认策略保护。
- 支持 PLAINTEXT、TLS、SASL/PLAIN、SCRAM、Kerberos/GSSAPI 和 OAUTHBEARER。
- 支持 Confluent 兼容及 AWS Glue Schema Registry。
- 支持 ZooKeeper broker 发现、JSON/CSV 导出和 MCP 自动化接口。
- 界面支持简体中文、繁体中文、英语、西班牙语、意大利语、日语和葡萄牙语。

工作台截图将在后续版本补充；插件图标见 `assets/plugin.svg`。

## MCP 自动化

独立 stdio 模式启动：

```bash
backend/bin/dbx-plugin-kafka --mcp
```

Kafka MCP 默认只读。常用工具包括 `kafka_messages_digest`、
`kafka_cursor_next`、`kafka_messages_produce` 和消费组 offset 工具；生产消息
需显式传 `readOnly: false`，删除或清理操作还需 `allowDelete: true`。
完整配置见 [Kafka MCP 参考](docs/MCP.zh-CN.md)。

## 安全设计

SASL、TLS、Kerberos、AWS 和 Schema Registry 凭据由 DBX 宿主 secret binding 管理，
插件不持久化敏感信息。生产连接建议优先使用 TLS、只读模式和最小权限 ACL，
对删除 topic、消费组和 ACL 等高风险操作启用确认策略。

## 安装

从 [GitHub Releases](https://github.com/jinpy666/dbx-plugin-kafka/releases) 下载匹配平台的
`.dbxp` 包，在 DBX 插件中心选择本地安装。开发者也可以按照
[迁移与发布说明](docs/REPOSITORY_SPLIT.zh-CN.md) 构建候选包。

## 开发与验证

```bash
pnpm --dir frontend install
pnpm --dir frontend typecheck && pnpm --dir frontend test && pnpm --dir frontend build
(cd backend && go vet ./... && go test ./...)
python3 scripts/validate_repo.py && node scripts/connection-forms/verify.mjs kafka
scripts/test.sh
```

协议、Schema Registry 和集成验证说明位于 `docs/`；独立仓库的迁移边界、
公共依赖和发布前置条件见[迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md)。
