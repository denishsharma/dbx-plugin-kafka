# DBX Kafka

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[中文](README.md) · [Repository split notes](docs/REPOSITORY_SPLIT.en.md) · [Kafka MCP reference](docs/MCP.zh-CN.md)

DBX Kafka is a visual workspace for Apache Kafka operations and troubleshooting.
It brings topics, messages, consumer groups, ACLs, and Schema Registry into one
interface for everyday development, testing, and production administration.

## Use cases

- Confirm topic, partition, consumer-group, and cluster metadata quickly.
- Investigate payload formats, filters, consumer lag, and offset behavior.
- Produce messages, adjust offsets, and manage ACLs behind read-only and confirmation safeguards.

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

Workspace screenshots will be added in a later release; the plugin icon lives at
`assets/plugin.svg`.

## MCP automation

Start standalone stdio mode with:

```bash
backend/bin/dbx-plugin-kafka --mcp
```

Kafka MCP is read-only by default. Useful tools include `kafka_messages_digest`,
`kafka_cursor_next`, `kafka_messages_produce`, and consumer-group offset tools.
Producing requires `readOnly: false`; deletion and clearing also require
`allowDelete: true`. See the [Kafka MCP reference](docs/MCP.zh-CN.md).

## Security

SASL, TLS, Kerberos, AWS, and Schema Registry credentials are managed through DBX
host secret bindings and are not persisted by the plugin. For production clusters,
prefer TLS, read-only mode, and least-privilege ACLs; require confirmation for
topic, consumer-group, and ACL deletion operations.

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

Protocol, Schema Registry, and integration details live under `docs/`. The
repository split notes describe the split boundary, vendored dependencies, and
release preconditions for this standalone repository.
