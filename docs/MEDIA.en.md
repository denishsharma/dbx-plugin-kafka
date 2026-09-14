# DBX Kafka showcase assets

This page collects copy-ready, text-only assets for GitHub, release notes, and
product introductions. All statements reflect the current implementation. The
repository does not ship screenshots or demo videos yet; do not reference media
files that do not exist, and expect this page to grow when assets land.

## One-line positioning

`DBX Kafka: topic inspection, message search, consumer-group monitoring, schema checks, and guarded write operations in one DBX workspace.`

## Copy-ready messaging

Short: `DBX Kafka — manage Apache Kafka from the DBX workspace: topics, messages, consumer groups, ACLs, and Schema Registry in one interface.`

Long: `DBX Kafka is a visual workspace for Apache Kafka operations and troubleshooting.
Browse topics, partitions, ISR, and cluster metadata; produce and consume messages with
multi-channel filters, field search, offset strategies, Base64 handling, and common
compression/decoding; stream records with pause, resume, buffering, and JSON/CSV export;
watch consumer-group lag, members, and offsets with controlled, auditable resets.
PLAINTEXT, TLS, SASL (PLAIN/SCRAM), Kerberos/GSSAPI, and OAUTHBEARER are supported, plus
Confluent-compatible and AWS Glue Schema Registry. Credentials stay in DBX host secret
bindings and are never persisted by the plugin; write operations sit behind read-only
and delete-confirmation gates, and the MCP write path enforces two-phase confirmation.
The UI ships in seven languages.`

## Three reasons to try it

- **From connection to conclusion**: topics, partitions, consumer groups, messages, and
  schemas share one connection context — no more switching between CLI tools and web consoles.
- **From usable to governable**: read-only mode, the `allowDelete` gate, delete
  confirmations, and two-phase MCP writes put high-risk operations behind explicit
  permission boundaries, with the write path recorded in the audit log.
- **From manual to automated**: 11 MCP tools reuse saved connections and policy; digest
  aggregation with cursor paging hands AI conclusions instead of raw data, and scanned
  message bodies never leave the sidecar.

## Capability snapshot

- Topic inspection: list, partitions, ISR, configs, cluster metadata; ZooKeeper broker discovery.
- Message search: one-shot consumption (`maxScanRecords` semantics) with sidecar-local
  aggregation; key/value/header filter channels, matchMode, offset strategies
  (latest/earliest/committed/timestamp/offset), Base64 and gzip/lz4/zstd/snappy
  decompression, Confluent wire-format decoding.
- Streaming consumption: pause, resume, buffering, JSON/CSV export.
- Consumer groups: lag, members, and offset views; controlled offset resets
  (earliest/latest/timestamp/partitionOffset).
- Administration: topic create/delete, ACL management, record clearing; configurable
  read-only and delete-confirmation policy.
- Schema Registry: Confluent-compatible and AWS Glue; browse subjects/versions and
  participate in message decoding.
- Automation: 11 MCP tools (UI intents, digest/cursor, produce, two-phase writes) via
  standalone stdio mode or the DBX MCP bridge.
- Internationalization: Simplified Chinese, Traditional Chinese, English, Spanish,
  Italian, Japanese, and Portuguese.

## Scope note

This page describes the plugin UI and sidecar offline/online capabilities. Live Kafka
clusters, Schema Registry, the DBX.app host bridge, and the Docker test cluster are
separate runtime environments; container scenarios in the MCP smoke
(`python3 scripts/smoke_mcp.py`) report `SKIP` when their environment is absent.
Use CI results, smoke output, and release notes for verified claims.
