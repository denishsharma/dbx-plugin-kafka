# DBX Kafka

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[中文](README.md) · [Showcase](docs/MEDIA.en.md) · [Feature comparison](docs/COMPARISON.en.md) · [MCP guide](docs/MCP_USAGE.en.md) · [Repository split notes](docs/REPOSITORY_SPLIT.en.md)

DBX Kafka is a visual workspace for Apache Kafka operations and troubleshooting.
Open one connection and move from topic inspection to message search, consumer-group
observation, schema checks, and guarded write operations, then hand the same
capabilities to AI automation through MCP tools. It turns the next hour after
“connecting to a cluster” into one coherent, reusable, auditable workflow.

> Topics · Messages · Consumer Groups · Schema Registry: one DBX workspace for everyday Kafka operations.

## Why teams reach for it

| Your job | The DBX Kafka workflow |
| --- | --- |
| Inspect clusters and topics quickly | Topics, partitions, ISR, configs, consumer groups, and cluster metadata in one view |
| Investigate messages and lag | Multi-channel filters, offset strategies, message detail drawer, and consumer-group lag views |
| Change data with guardrails | Producing, offset resets, and topic/ACL changes stay behind read-only and confirmation gates |
| Check streaming data | Stream consumption with pause, resume, buffering, and JSON/CSV export |
| Let AI query Kafka for you | 11 MCP tools reuse connections and permission boundaries; aggregated results save tokens |

## Use cases

- Confirm topic, partition, consumer-group, and cluster metadata in dev and test environments.
- Investigate payload formats, filters, consumer lag, and offset behavior without juggling CLI tools.
- Produce messages, adjust offsets, and manage topics and ACLs behind read-only and delete-confirmation safeguards.
- Let AI clients reuse saved connections over MCP for routine inspection and troubleshooting.

## Highlights

- Inspect topics, partitions, ISR, configuration, and cluster metadata.
- Produce and consume messages with filtering, field search, offset strategies,
  Base64 handling, and common compression/decoding options.
- Stream records with pause, resume, buffering, and export.
- Inspect consumer-group lag, members, and offsets, with controlled offset reset.
- Manage topics, ACLs, and related configuration with read-only and delete safeguards.
- PLAINTEXT, TLS, SASL/PLAIN, SCRAM, Kerberos/GSSAPI, and OAUTHBEARER support.
- Confluent-compatible and AWS Glue Schema Registry support.
- ZooKeeper broker discovery, JSON/CSV export, and MCP automation interfaces.
- Simplified Chinese, Traditional Chinese, English, Spanish, Italian, Japanese,
  and Portuguese UI.

See the [feature and competitor comparison](docs/COMPARISON.en.md) for how this
positions against kcat, kafka-console-consumer, and popular Kafka web UIs; the
[showcase page](docs/MEDIA.en.md) collects copy-ready messaging.

## MCP automation

Use the DBX MCP bridge to reuse saved connections, credential resolution, and
permission policy. For standalone access:

```bash
backend/bin/dbx-plugin-kafka --mcp
```

Kafka MCP is read-only by default and ships 11 tools: `kafka_messages_digest`
(local aggregation with cursor paging), `kafka_cursor_next`,
`kafka_messages_produce`, `kafka_groups_offsets_reset`, `kafka_topics_delete`,
`kafka_topics_records_clear`, plus five UI tools. Producing requires
`readOnly: false`; deletion and clearing also require `allowDelete: true` and a
two-phase confirmation. See the [MCP guide](docs/MCP_USAGE.en.md) and the
[Kafka MCP reference](docs/MCP.zh-CN.md) for configuration and safety details.

## Security

SASL, TLS, Kerberos, AWS, and Schema Registry credentials are managed through DBX
host secret bindings and are not persisted by the plugin. For production clusters,
prefer TLS, read-only mode, and least-privilege ACLs; require confirmation for
topic, consumer-group, and ACL deletion operations. MCP write tools enforce a
preview → confirmToken two-phase flow and are recorded in the audit log.

## Install

Download the `.dbxp` package for your platform from
[GitHub Releases](https://github.com/jinpy666/dbx-plugin-kafka/releases) and
install it locally from the DBX plugin center. Developers can build candidate
packages by following the [repository split notes](docs/REPOSITORY_SPLIT.en.md).

## Development

```bash
pnpm --dir frontend install
pnpm --dir frontend typecheck && pnpm --dir frontend test && pnpm --dir frontend build
(cd backend && go vet ./... && go test ./...)
python3 scripts/validate_repo.py && node scripts/connection-forms/verify.mjs kafka
scripts/test.sh
```

Start a local Kafka/Schema Registry test cluster with `scripts/dev-cluster.sh`
(Docker); run the MCP smoke with `python3 scripts/smoke_mcp.py` (container
scenarios honestly SKIP when their environment is absent).

## Documentation

- [Implementation plan](docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md): scope and iteration log.
- [Protocol reference](docs/PROTOCOL_KAFKA.zh-CN.md): sidecar methods and events.
- [Kafka MCP reference](docs/MCP.zh-CN.md): tool contracts, inline credentials, and the app-bridge fallback.
- [MCP guide](docs/MCP_USAGE.en.md) / [Chinese](docs/MCP_USAGE.zh-CN.md): onboarding and common pitfalls.
- [Showcase](docs/MEDIA.en.md) / [Chinese](docs/MEDIA.zh-CN.md): copy-ready messaging.
- [Feature comparison](docs/COMPARISON.en.md) / [Chinese](docs/COMPARISON.zh-CN.md): positioning against alternatives.
- [Repository split notes](docs/REPOSITORY_SPLIT.en.md) / [Chinese](docs/REPOSITORY_SPLIT.zh-CN.md).
