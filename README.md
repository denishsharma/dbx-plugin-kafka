# Kafka Studio

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[English](README.en.md) · [产品宣传页](docs/MEDIA.zh-CN.md) · [特性与竞品对比](docs/COMPARISON.zh-CN.md) · [MCP 使用指南](docs/MCP_USAGE.zh-CN.md) · [独立仓库迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md)

Kafka Studio 是面向 Apache Kafka 运维与排障的可视化工作台。打开一个连接，就能完成
topic 巡检、消息检索、消费组观测、Schema 查看和受控的写操作，并通过 MCP 工具把
这些能力交给 AI 自动化。它把“登录集群之后的下一小时”压缩成一个连贯、可审计、
可复用的工作流。

> Topics · Messages · Consumer Groups · Schema Registry：一个 DBX 工作台管完 Kafka 日常运维。

![Kafka Studio 功能演示](docs/media/kafka-studio-demo.mp4)

## 为什么值得用

| 你要完成的事 | Kafka Studio 给你的体验 |
| --- | --- |
| 快速巡检集群与 topic | topic、分区、ISR、配置、消费组与集群元数据一屏可见 |
| 排查消息与消费延迟 | 多通道过滤、offset 策略、消息详情抽屉与消费组 lag 视图 |
| 受控地改数据 | 生产消息、offset 重置、topic/ACL 变更都在只读与确认门之内 |
| 检查流式数据 | 流式消费支持暂停、恢复、缓冲与 JSON/CSV 导出 |
| 让 AI 帮你查 Kafka | 11 个 MCP 工具复用连接与权限边界，聚合结果省 token |

## 适合场景

- 开发与测试环境快速确认 topic、分区、消费组和集群元数据是否符合预期。
- 排查消息格式、过滤条件、消费延迟和 offset 问题，无需在多个命令行工具间切换。
- 在只读与删除确认策略保护下完成消息生产、offset 调整、topic 与 ACL 运维。
- 用 AI 客户端经 MCP 复用已保存连接，完成日常巡检和排障。

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

与 kcat、kafka-console-consumer、各类 Kafka Web UI 的定位对比见
[特性与竞品对比](docs/COMPARISON.zh-CN.md)；更多宣传文案见
[产品宣传页](docs/MEDIA.zh-CN.md)。

## MCP 自动化

推荐通过 DBX MCP 桥调用，以复用已保存连接、凭据解析和权限策略。独立模式可运行：

```bash
backend/bin/dbx-plugin-kafka --mcp
```

Kafka MCP 默认只读，共 11 个工具：`kafka_messages_digest`（本地聚合 + cursor 翻页）、
`kafka_cursor_next`、`kafka_messages_produce`、`kafka_groups_offsets_reset`、
`kafka_topics_delete`、`kafka_topics_records_clear`，以及 5 个 UI 类工具。
生产消息需显式传 `readOnly: false`，删除或清理操作还需 `allowDelete: true` 并走
两阶段确认。配置、工具语义和安全边界见
[MCP 使用指南](docs/MCP_USAGE.zh-CN.md)与[Kafka MCP 参考](docs/MCP.zh-CN.md)。

## 安全设计

SASL、TLS、Kerberos、AWS 和 Schema Registry 凭据由 DBX 宿主 secret binding 管理，
插件不持久化敏感信息。生产连接建议优先使用 TLS、只读模式和最小权限 ACL，
对删除 topic、消费组和 ACL 等高风险操作启用确认策略；MCP 写操作强制
preview → confirmToken 两阶段确认并落审计日志。

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

本地 Kafka/Schema Registry 测试集群用 `scripts/dev-cluster.sh` 拉起（Docker）；
MCP 离线冒烟用 `python3 scripts/smoke_mcp.py`（容器类用例在环境不可用时诚实 SKIP）。

## 文档索引

- [实施计划](docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md)：能力范围与轮次记录。
- [协议文档](docs/PROTOCOL_KAFKA.zh-CN.md)：sidecar 协议方法与事件。
- [Kafka MCP 参考](docs/MCP.zh-CN.md)：工具契约、内联凭据与桥接兜底。
- [MCP 使用指南](docs/MCP_USAGE.zh-CN.md) / [英文版](docs/MCP_USAGE.en.md)：接入与常见坑。
- [产品宣传页](docs/MEDIA.zh-CN.md) / [英文版](docs/MEDIA.en.md)：可直接引用的宣传素材。
- [特性与竞品对比](docs/COMPARISON.zh-CN.md) / [英文版](docs/COMPARISON.en.md)：定位对比。
- [独立仓库迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md) / [英文版](docs/REPOSITORY_SPLIT.en.md)。
