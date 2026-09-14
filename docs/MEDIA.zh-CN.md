# DBX Kafka 产品宣传素材

本页集中提供可直接用于 GitHub、发布说明和产品介绍的纯文字素材。所有描述以
当前实现为准；仓库暂未收录截图或演示视频，请勿引用不存在的媒体文件，
素材补充后会更新本页。

## 一句话定位

`DBX Kafka：把 topic 巡检、消息检索、消费组观测、Schema 查看和受控写操作集中到一个 DBX 工作台。`

## 宣传文案

短版：`DBX Kafka——在 DBX 工作台中管理 Apache Kafka：topic、消息、消费组、ACL 和 Schema Registry 一个界面管完。`

长版：`DBX Kafka 是面向 Apache Kafka 运维与排障的可视化工作台。浏览 topic、分区、ISR
与集群元数据；生产与消费消息，支持多通道过滤、字段检索、offset 策略、Base64 与常见
压缩/解码；流式消费支持暂停、恢复、缓冲与 JSON/CSV 导出；消费组 lag、成员与 offset
一目了然，重置操作受控可审计；支持 PLAINTEXT、TLS、SASL（PLAIN/SCRAM）、
Kerberos/GSSAPI、OAUTHBEARER，以及 Confluent 兼容与 AWS Glue Schema Registry。
凭据由 DBX 宿主 secret binding 管理，插件不持久化；写操作受只读与删除确认门保护，
MCP 写路径强制两阶段确认。界面支持七语。`

## 三句卖点

- **从连接到结论**：topic、分区、消费组、消息和 Schema 在同一个连接上下文里查看，
  不用在命令行工具和网页控制台之间来回切换。
- **从能用到敢用**：只读模式、`allowDelete` 门、删除确认与 MCP 两阶段确认
  把高风险操作放进明确的权限边界，写路径落审计日志。
- **从手动到自动化**：11 个 MCP 工具复用已保存连接与策略；digest 本地聚合 +
  cursor 翻页让 AI 拿到结论而不是全量数据，扫描过程消息不出 sidecar。

## 能力速览

- Topic 巡检：列表、分区、ISR、配置、集群元数据；支持 ZooKeeper broker 发现。
- 消息检索：一次性消费（`maxScanRecords` 语义）+ sidecar 本地聚合，key/value/header
  过滤通道、matchMode、offset 策略（latest/earliest/committed/timestamp/offset）、
  Base64 与 gzip/lz4/zstd/snappy 解压、Confluent wire format 解码。
- 流式消费：暂停、恢复、缓冲、JSON/CSV 导出。
- 消费组：lag、成员、offset 视图；受控 offset 重置（earliest/latest/timestamp/partitionOffset）。
- 运维：topic 创建/删除、ACL 管理、记录清理；只读与删除确认策略可配。
- Schema Registry：Confluent 兼容与 AWS Glue；浏览 subject/version 并参与消息解码。
- 自动化：11 个 MCP 工具（UI intent、digest/cursor、produce、两阶段写），stdio
  独立模式与 DBX MCP 桥两种接入。
- 国际化：简体中文、繁体中文、英语、西班牙语、意大利语、日语、葡萄牙语。

## 使用边界

本页描述插件 UI 与 sidecar 离线/在线能力。真实 Kafka 集群、Schema Registry、
DBX.app 宿主桥和 Docker 测试容器需要对应运行环境；MCP 离线冒烟
（`python3 scripts/smoke_mcp.py`）中容器类用例在环境不可用时按 `SKIP` 报告，
请以 CI、smoke 输出和发布说明中的实际验证结果为准。
