<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# nats-lua ADR compliance

Source: [nats-io/nats-architecture-and-design](https://github.com/nats-io/nats-architecture-and-design),
checked 2026-10-12. Included: client-relevant ADRs with status Approved,
Implemented or Partially Implemented. Excluded: Proposed/Draft (53, 54, 57, 58),
Deprecated (15, 18, 52) and server-only ADRs (3, 12, 26, 28, 30, 38, 39, 41, 44,
55, 56, 59, 61, 62).

Legend: ✅ done, ◐ partial, ❌ missing, n/a = nothing for a client to do.

| ADR | Title | Status | 0.2.0 | 0.3.0 | Notes |
|---|---|---|---|---|---|
| 1 | JetStream JSON API Design | Implemented | ✅ | ✅ | API paging now aggregated (stream/consumer lists, names) |
| 2 | NATS Typed Messages | Implemented | ◐ | ◐ | JSON encoded client; header/status constants; no typed-message helpers |
| 4 | NATS Message Headers | Implemented | ✅ | ✅ | + `nats.headers` / `nats.status` constants |
| 5 | Lame Duck Notification | Partially Impl. | ✅ | ✅ | callback, no automatic migration |
| 6 | Naming Rules | Approved | ✅ | ✅ | |
| 7 | NATS Server Error Codes | Partially Impl. | ✅ | ✅ | `msg.data.error.err_code` |
| 8 | JetStream Key-Value Stores | Implemented | ◐ | ✅ | watch options, keys, history, purge_deletes, create over delete marker, mirrors/sources, bind to mirrors |
| 9 | Consumer Idle Heartbeats | Approved | ◐ (fetch only) | ✅ | push FC replies, missed-heartbeat detection in consume/push/ordered |
| 10 | JetStream Extended Purge | Implemented | ✅ | ✅ | |
| 11 | Hostname resolution | Approved | ❌ | ✅ | resolve all addresses, shuffle, IPv6 |
| 13 | Pull Subscribe internals | Partially Impl. | ✅ | ✅ | + 423 handling |
| 14 | JWT library free user generation | Approved | ❌ | ✅ | `nats-nkeys.issue_user_jwt`, verified by nats-server in operator mode |
| 17 | Ordered Consumer | Implemented | ❌ | ✅ | pull (fetch/next/consume/messages) and push |
| 19 | API prefixes / domains | Partially Impl. | ◐ | ✅ | `api_prefix`/`domain` options |
| 20 | Object Stores | Implemented | ✅ | ✅ | + non-blocking watcher |
| 21 | Configuration Contexts | Partially Impl. | ❌ | ✅ | `nats.load_context`, `connect_context` (no nsc lookup) |
| 22 | Publish retries on no responders | Partially Impl. | ✅ | ✅ | |
| 31 | Direct Get | Implemented | ◐ | ✅ | batch, multi_last, up_to_seq/time, EOB |
| 32 | Service API | Implemented | ❌ | ✅ | `nats-micro`, discovered by `nats micro` and nats.go |
| 33 | Metadata for Stream and Consumer | Approved | ✅ | ✅ | |
| 34 | Consumers Multiple Filters | Approved | ✅ | ✅ | `filter_subjects` |
| 35 | Filestore Compression | Implemented | ✅ | ✅ | config passthrough; KV uses s2 when asked |
| 36 | Subject Mapping Transforms | Implemented | ✅ | ✅ | config passthrough; KV sources use transforms |
| 37 | JetStream Simplification | Approved | ❌ | ✅ | consume/messages, thresholds, stop/drain; context is the client itself |
| 40 | NATS Connection | Implemented | ◐ | ✅ | non-blocking reconnect with buffer; WebSocket transport |
| 42 | Pull Consumer Priority Groups | Approved | ❌ | ✅ | pause/resume, pinned client, overflow, unpin |
| 43 | Per-Message TTL | Implemented | ❌ | ✅ | `ttl` option on publish |
| 47 | Request Many | Partially Impl. | ◐ | ✅ | `request_many` |
| 48 | KV TTL / limit markers | Implemented | ❌ | ✅ | `limit_marker_ttl`, per key TTL on create/purge |
| 49 | Distributed Counter CRDT | Approved | ❌ | ✅ | `js_counter_add`, `counter_get` |
| 50 | Batch Publishing | Approved | ❌ | ◐ | atomic batches done; fast-ingest batches not done |
| 51 | Message Scheduler | Approved | ❌ | ◐ | headers passthrough only |
| 60 | Reliable sourcing on WQ/Interest | Implemented | n/a | n/a | server behaviour, config passthrough |
| 63 | Stream Backup and Restore V2 | Implemented | ❌ | ❌ | not done |

## Remaining work

- ADR-50 fast-ingest batch publishing (flow-controlled), ADR-51 scheduler helpers.
- ADR-63 backup/restore.
- WebSocket permessage-deflate compression.
- No background threads: push flow control, heartbeats and watchers need the application to pump (`nc:wait`).
