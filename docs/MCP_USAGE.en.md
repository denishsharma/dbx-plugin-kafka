# Kafka MCP usage guide

For the full tool contract (parameters, tolerance semantics, the inline-credential
table, and the app-bridge fallback) see the [Kafka MCP reference](MCP.zh-CN.md).
This page is the quick start: how to connect, what the tools are, where the safety
gates sit, and how to escape common pitfalls.

## Two ways to connect

1. **DBX MCP bridge (recommended)**: AI clients call the plugin protocol methods
   `mcp/tools` and `mcp/call` through the host's `dbx_list_plugin_tool`s /
   `dbx_call_plugin_tool`. The bridge forwards the lifecycle payload per
   `connectionId`, the host resolves credentials, no password ever appears in tool
   arguments, and the connection's read-only/delete policy is inherited automatically.
2. **Standalone stdio mode (`--mcp`)**: the sidecar binary runs its own MCP server
   (MCP `2024-11-05`, newline-delimited JSON-RPC over stdio) with no DBX required.
   Connection parameters are passed inline per call (never persisted; pooled in
   process memory by a hash of the parameters).

## Standalone stdio setup

```bash
backend/bin/dbx-plugin-kafka --mcp
```

Example configuration for ZCode / generic MCP clients:

```json
{
  "mcp": {
    "servers": {
      "dbx-kafka": {
        "type": "stdio",
        "command": "/absolute/path/backend/bin/dbx-plugin-kafka",
        "args": ["--mcp"]
      }
    }
  }
}
```

In stdio mode the five UI tools (`kafka_ui_*`) are listed but return an explicit
`UNAVAILABLE` (they need the DBX workbench). Connection tools accept inline
credentials, or forward an unpooled `connectionId` to a running DBX app through the
local TCP bridge (fail-closed — no hangs, no silent redials).

## Tool inventory (11)

| Tool | Class | Semantics (one line) |
| --- | --- | --- |
| `kafka_messages_digest` | Read | One-shot scan with sidecar-local aggregation (counts/distributions/sample/`cursorId`); `format:"rows"` returns ≤20 rows; message bodies never leave the sidecar |
| `kafka_cursor_next` | Read | Page through a digest session: conditions are not re-sent, the backend is not re-scanned; ≤20 rows per batch, TTL defaults to 600s |
| `kafka_ui_search` | UI intent | Fill the consume form in the messages panel and trigger consumption; results stay in the UI (pending + digest hint if the frontend does not answer) |
| `kafka_ui_focus` | UI intent | Focus the messages/topics/groups/schemas panel |
| `kafka_ui_select` | UI intent | Locate a hit row by partition+offset in the current results and open the detail drawer |
| `kafka_ui_state` | UI intent | Read an intent result or the latest UI snapshot (with stream-session status) |
| `kafka_ui_topics` | Discovery | Topic name list (hard cap 50, locator-only) |
| `kafka_messages_produce` | Write | Single small message, single-phase execution (key+value ≤64 KiB); audited with `source:"mcp"` |
| `kafka_topics_delete` | Write | Delete topics; mandatory two-phase preview → confirmToken |
| `kafka_groups_offsets_reset` | Write | Consumer-group offset reset; mandatory two-phase with per-mode parameter pre-checks |
| `kafka_topics_records_clear` | Write | Clear topic records; mandatory two-phase |

Inline connection parameters for connection tools: `brokers` (required),
`securityProtocol`, `saslMechanism`, `saslUsername`/`saslPassword`,
`tlsCaCert`/`tlsClientCert`/`tlsClientKey`,
`schemaRegistry`/`schemaRegistryUrl`/`schemaRegistryUsername`/`schemaRegistryPassword`,
`clientId`, `readOnly`, `allowDelete` (camelCase, aligned with the connection form).

## Safety gates (read first)

- **Read-only by default**: stdio inline connections default `readOnly` to `true`;
  write tools are refused until you resend with explicit `"readOnly": false`.
- **One more gate for deletes**: `kafka_topics_delete` and
  `kafka_topics_records_clear` additionally require `"allowDelete": true`.
- **Two-phase confirmation**: the three high-risk write tools first return a preview
  plus a `confirmToken` (one-time, 60s, bound to the parameter hash); confirm by
  resending the same arguments with the token.
- **Response economy**: a single response is capped at 16 KiB; oversized values appear
  as placeholders with partition/offset locators and never leave MCP; digest defaults
  to `format:"digest"`; `maxScanRecords` defaults to 1000 with a 100000 ceiling.
- **Audit**: the MCP write path appends to `audit.jsonl` in the data directory
  (`source:"mcp"`).

## Common pitfalls

- **Missing-parameter errors** enumerate every gap at once
  (`Missing required parameters: connectionId, topic`) — fill them all in one round.
- **Writes refused**: resend with `"readOnly": false` (plus `"allowDelete": true` for
  delete-class tools); this is the most common blocker in live agent testing.
- **Unsure about topic names**: call `kafka_ui_topics` (workbench mode) or get them
  from the user/cluster docs; when a digest scans zero records the sidecar runs a
  topic-existence check and reports missing topics explicitly.
- **Expired cursor**: the error carries the effective TTL; resend
  `kafka_messages_digest` for a fresh `cursorId`.
- **UI tools report UNAVAILABLE on stdio**: by design — use digest/cursor instead, or
  call through the DBX MCP bridge in workbench mode.
- **Produce over 64 KiB**: the error includes the actual size and the limit; route
  large payloads through the workbench.
- **Enum case**: `format`, `panel`, `offsetStrategy`, `matchMode`, and `resetTo` are
  case-insensitive; string booleans accept `true/false/1/0/yes/no/on/off`.

## Verification

Offline smoke (no live cluster needed):

```bash
DBX_PLUGIN_SIDECAR=backend/bin/dbx-plugin-kafka python3 scripts/smoke_mcp.py
```

Container scenarios (K11/K13/K14/K15/K18/K19) need the local test cluster from
`scripts/dev-cluster.sh`; they report `SKIP` when the environment is absent. An
offline pass never counts as a live-connection pass — trust the script output for both.
