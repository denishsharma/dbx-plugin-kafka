# Kafka MCP 使用说明

工具契约全文（参数、容错语义、内联凭据表、桥接兜底）见
[Kafka MCP 参考](MCP.zh-CN.md)。本文是接入速查：怎么起、有哪些工具、
安全门在哪、常见坑怎么解。

## 两种接入方式

1. **DBX MCP 桥（推荐）**：AI 客户端经宿主的 `dbx_list_plugin_tools` /
   `dbx_call_plugin_tool` 调用插件协议方法 `mcp/tools`、`mcp/call`。桥按
   `connectionId` 转发 lifecycle payload，凭据由宿主解析，工具参数里不出现
   任何密码，并自动继承连接的只读/删除策略。
2. **独立 stdio 模式（`--mcp`）**：sidecar 二进制自起 MCP 服务器
   （MCP `2024-11-05`，换行分隔 JSON-RPC over stdio），无需 DBX 在场，
   连接参数随每次调用内联传入（不落盘，按参数 hash 池化复用）。

## 独立 stdio 模式接入

```bash
backend/bin/dbx-plugin-kafka --mcp
```

ZCode / 通用 MCP 客户端配置示例：

```json
{
  "mcp": {
    "servers": {
      "dbx-kafka": {
        "type": "stdio",
        "command": "/绝对路径/backend/bin/dbx-plugin-kafka",
        "args": ["--mcp"]
      }
    }
  }
}
```

stdio 模式下 5 个 UI 类工具（`kafka_ui_*`）照常列出但返回明确
`UNAVAILABLE`（需要 DBX 工作台）；连接类工具支持内联凭据，或把未池化的
`connectionId` 经本地 TCP 桥转发给运行中的 DBX 应用（fail-closed，
不假死不静默重拨）。

## 工具一览（11 个）

| 工具 | 类别 | 语义（一行版） |
| --- | --- | --- |
| `kafka_messages_digest` | 读 | 一次性扫描 + sidecar 本地聚合（计数/分布/样本/`cursorId`）；`format:"rows"` 出 ≤20 行；消息体不出 sidecar |
| `kafka_cursor_next` | 读 | digest 会话翻页：条件不重发、远端不重扫，单批 ≤20 行，TTL 缺省 600s |
| `kafka_ui_search` | UI intent | 把消费条件填进消息面板并触发消费，结果留在 UI（前端未响应 → pending + 引导走 digest） |
| `kafka_ui_focus` | UI intent | 聚焦 messages/topics/groups/schemas 面板 |
| `kafka_ui_select` | UI intent | 按 partition+offset 定位消息结果中的命中行并打开详情 |
| `kafka_ui_state` | UI intent | 读 intent 结果或最新 UI 快照（可附 stream 会话状态） |
| `kafka_ui_topics` | 元发现 | topic 名清单（硬上限 50，纯定位用） |
| `kafka_messages_produce` | 写 | 单条小消息单阶段直执行（value/key 合计 ≤64 KiB）；审计 `source:"mcp"` |
| `kafka_topics_delete` | 写 | 删除 topic，强制两阶段 preview → confirmToken |
| `kafka_groups_offsets_reset` | 写 | 消费组 offset 重置，强制两阶段；按 resetTo 模式预检配套参数 |
| `kafka_topics_records_clear` | 写 | 清空 topic 记录，强制两阶段 |

连接类工具内联参数：`brokers`（必填）、`securityProtocol`、`saslMechanism`、
`saslUsername`/`saslPassword`、`tlsCaCert`/`tlsClientCert`/`tlsClientKey`、
`schemaRegistry`/`schemaRegistryUrl`/`schemaRegistryUsername`/`schemaRegistryPassword`、
`clientId`、`readOnly`、`allowDelete`（camelCase，与连接表单字段对齐）。

## 安全门（务必先读）

- **默认只读**：stdio 内联连接 `readOnly` 缺省 `true`，写工具一律拒绝；
  要写必须显式传 `"readOnly": false` 重发。
- **删除类再叠一门**：`kafka_topics_delete` 与 `kafka_topics_records_clear`
  还要求 `"allowDelete": true`。
- **两阶段确认**：三个高危写工具先调一次拿 preview + `confirmToken`
  （一次性、60s、参数 hash 绑定），再用同一参数加 token 确认执行。
- **响应经济**：单响应上限 16 KiB；大 value 以占位符 + partition/offset
  定位指引出现，原文不出 MCP；digest 缺省 `format:"digest"`，
  扫描上限 `maxScanRecords` 缺省 1000、上限 100000。
- **审计**：MCP 写路径落 `audit.jsonl`（数据目录内，`source:"mcp"`）。

## 常见坑速查

- **报缺参**：`required` 参数缺失时一次枚举全部缺口
  （如 `Missing required parameters: connectionId, topic`），一轮补齐即可。
- **写被拒**：先补 `"readOnly": false`（删除类再加 `"allowDelete": true`），
  用相同参数重发；这是 agent 真机实测最常见的卡点。
- **topic 名不确定**：先调 `kafka_ui_topics`（工作台模式）或从用户/集群文档
  获取；digest 扫描为 0 时会自动做存在性校验，不存在的 topic 明确报错。
- **cursor 过期**：报文带实际生效 TTL，重发一次 `kafka_messages_digest`
  拿新 `cursorId` 即可。
- **UI 工具在 stdio 报 UNAVAILABLE**：这是设计行为；改用 digest/cursor，
  或经 DBX MCP 桥以工作台模式调用。
- **produce 超 64 KiB**：报错带实际字节数与限额，大载荷请走工作台。
- **枚举大小写**：`format`、`panel`、`offsetStrategy`、`matchMode`、`resetTo`
  大小写不敏感；字符串布尔接受 `true/false/1/0/yes/no/on/off`。

## 验证

离线冒烟（无需真实集群）：

```bash
DBX_PLUGIN_SIDECAR=backend/bin/dbx-plugin-kafka python3 scripts/smoke_mcp.py
```

容器类用例（K11/K13/K14/K15/K18/K19）需要 `scripts/dev-cluster.sh` 拉起的
本地测试集群；环境不可用时按 `SKIP` 报告——离线通过不代表 live 连接通过，
两者都以脚本输出为准。
