<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# nats-lua API reference (v0.3.0)

Reference for the public API of every module, derived from the source. Start
with [the pump model](nats.md#the-pump-model-threading): nothing happens in the
background; callbacks, reconnects, keepalive, consumers, watchers and services
run only while the application is inside `nc:wait()` or another blocking call.

| Module | `require` returns | Reference | Contents | Functions |
|---|---|---|---|---|
| `nats` | `nats` | [nats.md](nats.md) | client, options, msg, encoded client, headers/status constants, JSON, contexts, WebSocket | 77 |
| `nats-jetstream` | `nats` | [nats-jetstream.md](nats-jetstream.md) | streams, consumers, fetch/consume/messages, ordered and push consumers, acks, direct get, counters, batches | 87 |
| `nats-kvs` | `nats` | [nats-kvs.md](nats-kvs.md) | Key-Value buckets, entries, watchers | 29 |
| `nats-objs` | `nats` | [nats-objs.md](nats-objs.md) | Object store, ObjectInfo, watchers | 36 |
| `nats-micro` | `nats` | [nats-micro.md](nats-micro.md) | Service API: services, groups, requests, discovery | 27 |
| `nats-nkeys` | own table | [nats-nkeys.md](nats-nkeys.md) | key pairs, ed25519, SHA-512, creds, user JWTs | 30 |
| `nats-sha256` | own table | [nats-sha256.md](nats-sha256.md) | SHA-256 | 6 |

Function counts are exported functions (metatable methods and module
functions) as counted by `tests/check_docs.lua`, which fails when an exported
function has no heading in its module page:

```sh
LUA_PATH="./?.lua;/path/to/lua-nuid/?.lua;;" lua tests/check_docs.lua
```

## Object types at a glance

| Object | Created by | Page |
|---|---|---|
| client | `nats.client`, `nats.connect`, `nats.connect_context` | [nats](nats.md#object-client) |
| msg | received messages, `publish`, `nats.msg` | [nats](nats.md#object-msg), [ack methods](nats-jetstream.md#object-msg-jetstream-methods) |
| encoded client | `client:encoded_client()` | [nats](nats.md#object-encoded-client-eclient) |
| stream | `client:stream()` and friends | [jetstream](nats-jetstream.md#object-stream) |
| consumer | `stream:consumer()` and friends | [jetstream](nats-jetstream.md#object-consumer) |
| consume context | `consumer:consume()` | [jetstream](nats-jetstream.md#object-consume-context-ctx) |
| message iterator | `consumer:messages()`, `ordered:messages()` | [jetstream](nats-jetstream.md#object-message-iterator-iter) |
| ordered consumer / context | `stream:ordered_consumer()`, `ordered:consume()` | [jetstream](nats-jetstream.md#object-ordered-consumer) |
| push subscription | `stream:subscribe()` | [jetstream](nats-jetstream.md#object-push-subscription-push_sub) |
| kv, entry, watcher | `client:kvs()`, `kv:get()`, `kv:watch()` | [kvs](nats-kvs.md) |
| obs, ObjectInfo, watcher | `client:create_object_store()`, `obs:get_info()`, `obs:watcher()` | [objs](nats-objs.md) |
| service, group, request | `client:add_service()`, `svc:add_group()`, handlers | [micro](nats-micro.md) |
| key pair | `nkeys.from_seed()`, `nkeys.create_pair()` | [nkeys](nats-nkeys.md#object-key-pair-kp) |
| hasher | `sha256.new()` | [sha256](nats-sha256.md#object-hasher) |

## API notes (known inconsistencies in 0.4.0)

These are documented as the code behaves today; they are candidates for a
future release. Fixed in 0.3.1: `stream:nextmsg` (now ADR-31 `next_by_subj`),
`client:new_inbox()` (honours `inbox_prefix`), `consumer:stop()/drain()`
(implemented), `nkeys.create_pair('curve')` (real x25519 xkeys with seal/open),
KV/object store/counter reads on streams without `allow_direct`. Fixed in
0.4.0: KV writes return the revision first and `kv:update` requires a revision
(ADR-8 / nats.go); JSON numbers without a fraction are integers on Lua 5.3+.

1. Error sentinels differ: most functions return `nil, err`, but
   `delete_stream`, `list_streams`, `stream:delete`, `purge`, `deletemsg`,
   `delete_consumer`, `consumer_list`, `consumer:delete` and the
   validation errors of `client:kvs` return `false, err`.
2. `msg:clone(subject, data, reply, header, sid)`: `subject`/`sid` override the
   message, while `data`/`reply`/`header` are only fallbacks.
3. `nats.load_context()` returns a `jetstream` table that `nats.client()` drops
   (not a valid option); pass its fields to `client:stream(..., opts)` yourself.
4. `nats.connect_context()` returns `nil, err` for context errors but raises
   for connection errors.
5. `stream:counter_add(subject, delta)` takes no `opts` (the client method does).
6. `sha256.hex(s)` hashes `s`; `nkeys.hex(s)` only hex-encodes `s`.
7. `nats.micro.ping/info/stats` return `nil, 'no responders'` (not `{}`) when no
   service answers.
8. `client:add_ticker()` returns the ticker table (the source comment says
    "id"); `on_reconnect`/`off_reconnect` are exported although meant for
    internal use.
9. `nats.jetstream.objs` is an alias of `nats.objs`; `max_chan_len` is accepted
    but unused.
10. `stream:getmsg/nextmsg/lastmsg` on a stream without `allow_direct` return the
    raw `STREAM.MSG.GET` API reply (base64 payload); use `stream:get_stored` for
    a uniform message.
