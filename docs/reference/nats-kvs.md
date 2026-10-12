<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-kvs` — Key-Value store

`local nats = require('nats-kvs')` (loads `nats-jetstream`; returns `nats`).
Buckets are streams `KV_<bucket>` on `$KV.<bucket>.>` (ADR-8). Reads use direct
get (`allow_direct`, falling back to `STREAM.MSG.GET` on buckets without it); watchers, `keys`, `history` and `purge_deletes` use
ordered consumers (ADR-17) driven by the pump loop — see
[the pump model](nats.md#the-pump-model-threading).

## Client methods

### client:kvs(cfg, opts, action)

Creates/updates a bucket and returns a [kv](#object-kv) handle.

| Param | Type | Default | Req | Notes |
|---|---|---|---|---|
| `cfg` | table or string (bucket name) | – | yes | see below |
| `opts` | table | `{}` | no | stream options (`domain`, `pre`) |
| `action` | `'create' \| 'update' \| nil` | nil = create or update | no | |

| `cfg` field | Type | Default | Notes |
|---|---|---|---|
| `bucket` | string | – (required) | `[%w-_]+` |
| `description` | string | – | |
| `history` | number 1..64 | 1 | |
| `ttl` | number (s) | 0 (none) | `max_age`; also sets `duplicate_window` |
| `max_bytes`, `max_value_size` | number | -1 | |
| `storage` | `'file' \| 'memory'` | `'file'` | |
| `replicas` | number | 1 | |
| `placement`, `republish`, `metadata` | table | – | passthrough |
| `compression` | `true \| 's2'` | none | |
| `limit_marker_ttl` | number (s, >= 1) | – | ADR-48: `allow_msg_ttl` + `subject_delete_marker_ttl`; enables per key TTL |
| `mirror` | `{name \| bucket, ...}` | – | mirror bucket (ADR-8/57 behaviour as nats.go) |
| `sources` | array of `{name \| bucket, filter_subject?, ...}` | – | subject transforms added automatically |

Returns: `kv, api_msg`. Errors: `false, 'no name provided for kvs create'`,
`false, 'bad bucket name'`, `false, 'history must be between 1 and 64'`,
`false, 'limit_marker_ttl must be at least 1 second'`, `nil, err` (stream
errors as in `client:stream`).

```lua
local kv = assert(nc:kvs({ bucket = 'config', history = 5 }))
```

### client:create_key_value(cfg, opts) / client:update_key_value(cfg, opts) / client:create_or_update_key_value(cfg, opts)

`client:kvs` with action `'create'` (fails when the bucket exists), `'update'`,
nil.

### client:key_value(bucket, opts)

Binds to an existing bucket. Returns: `kv` or `nil, 'bad bucket name' |
'bucket not found' | err`.

```lua
local kv = assert(nc:key_value('config'))
```

### client:delete_key_value(bucket, opts)

Returns: `true, api_msg`, `false, err`, or `nil, 'bad bucket name'`.

### client:key_value_store_names()

Returns: array of bucket names or `nil, err`.

## Object: kv

Fields: `name`/`bucket`, `stream`, `client`, `pre` (read subject prefix
`$KV.<bucket>.`; the origin's prefix for a mirror), `put_pre` (write prefix).

### Entry table

| Field | Type | Notes |
|---|---|---|
| `bucket`, `key` | string | |
| `value` | string or nil | nil for `DEL`/`PURGE` |
| `revision` | number | stream sequence |
| `created` | string | RFC 3339 (from `Nats-Time-Stamp` or the delivery time) |
| `created_ns` | number | ns timestamp (watch entries) |
| `delta` | number | pending after this entry (watch), 0 for `get` |
| `op` | `'PUT' \| 'DEL' \| 'PURGE'` | ADR-48 markers map `Remove` → DEL, `MaxAge`/`Purge` → PURGE |

### kv:get(key, revision)

Returns: entry; `nil, 'key not found'`; `nil, 'key not found', entry` for a
deleted/purged key (op DEL/PURGE); `nil, 'invalid key name'`; `nil, err`.
Buckets without `allow_direct` are read with `STREAM.MSG.GET` (0.3.1).

```lua
local e = kv:get('app.level'); if e then print(e.value, e.revision) end
```

### kv:get_revision(key, revision)

Same as `get(key, revision)`.

Error convention for writes: runtime and argument errors are returned, never
raised: `nil, err` (plus the reply `msg` and the JetStream `api_error` table
when the server rejected the write). Revisions are integers on Lua 5.3+.

### kv:put(key, value)

ADR-8 Put. Returns: `revision, msg` (`msg` is the pub ack reply, with `msg.ack`
and `msg.revision`); `nil, 'invalid key name'`; `nil, description, msg,
api_error`; `nil, 'timeout'`. Changed in 0.4.0 (returned `msg` first).

```lua
local rev = assert(kv:put('app.level', '3'))
```

### kv:update(key, value, last)

ADR-8 Update (compare-and-set): writes only if the last revision of `key` is
`last` (sent as `Nats-Expected-Last-Subject-Sequence`; `0` = the key must have
no stored message). `last` is required. Returns: `revision, msg`; `nil,
'revision required'` when `last` is missing or not a non-negative integer;
`nil, 'wrong last sequence', msg, api_error` (err_code 10071) for a stale
revision; other errors like `put`. Changed in 0.4.0 (returned `msg`; a missing
revision behaved like `put`).

```lua
local e = assert(kv:get('app.level'))
local rev, err = kv:update('app.level', '4', e.revision)
if err == 'wrong last sequence' then --[[ re-read and retry ]] end
```

### kv:create(key, value, opts)

ADR-8 Create: writes `key` only if it does not exist or its last entry is a
delete/purge marker (then it retries on top of the marker). `opts.ttl` (seconds
or string): per key TTL (ADR-48, bucket needs `limit_marker_ttl`). Returns:
`revision, msg`; `nil, 'key exists', msg, api_error`; other errors like `put`.

```lua
kv:create('lock.job1', 'me', { ttl = 30 })
```

### kv:delete(key, opts)

Writes a `DEL` marker. `opts.revision` (or `last_revision`): expected last
revision (stale → `nil, 'wrong last sequence', msg, api_error`); `opts.purge =
true` → `PURGE` with rollup; `opts.ttl` (with purge): marker TTL (ADR-48).
Returns: `true, msg` or `nil, err` (nats.go `Delete` returns only an error).

### kv:purge(key, opts)

`delete(key, {purge = true, ...})`: removes all history of `key`, leaving one
`PURGE` marker. Returns: `true, msg` or `nil, err`.

### kv:watch(keys, callback, opts)

Live watcher (ADR-8). `watch(callback, opts)` watches all keys.

| Param | Type | Default | Notes |
|---|---|---|---|
| `keys` | string (key or wildcard `a.*`, `>`) or array of filters (ADR-34) | `'>'` | |
| `callback` | function(entry \| nil) | – (required) | `nil` once after the initial values |
| `opts.include_history` | bool | false | deliver all revisions |
| `opts.ignore_deletes` | bool | false | skip DEL/PURGE |
| `opts.meta_only` | bool | false | headers only (`value` empty) |
| `opts.updates_only` | bool | false | only new changes (the nil marker fires immediately) |
| `opts.resume_from_revision` | number | – | start at this revision |
| `opts.max_messages`, `opts.idle_heartbeat`, `opts.expires` | number | 256, –, – | consume tuning |
| `opts.raw` | bool | false | 0.2 behaviour: plain subscription, returns a sid, raw msgs, live only |

Returns: [watcher](#object-watcher) (or a sid with `raw`), or
`nil, 'callback required' | 'invalid key' | err`. Entries arrive while the
client pumps.

```lua
local w = assert(kv:watch('app.*', function(e) if e then print(e.key, e.op, e.value) end end))
w:wait_initial(5)
while running do nc:wait({ timeout = 0.5 }) end
w:stop()
```

### kv:watch_all(callback, opts)

`watch('>', callback, opts)`.

### kv:keys(filters, opts)

Returns: array of current keys (deleted excluded), or `nil, 'timeout' | err`.
`filters`: string or array (default all). `opts.timeout` (default 10). Blocks
(pumps) until the initial data is complete. `keys(opts)` is accepted.

```lua
for _, k in ipairs(assert(kv:keys())) do print(k) end
```

### kv:list_keys(filters, opts)

Returns: an iterator function over `keys(...)`, or `nil, err`.

### kv:history(key, opts)

Returns: array of entries for `key`, oldest first, or `nil, 'key not found' |
'invalid key' | 'timeout'`. `opts`: `meta_only`, `ignore_deletes`, `timeout`.

### kv:purge_deletes(opts)

Removes delete/purge markers. Markers older than `opts.older_than` seconds
(default 1800; `<= 0` means all) are removed completely, newer ones keep only
the marker. Returns: number of keys processed, or `nil, err`.

### kv:status()

Returns: `{bucket, values, bytes, history, ttl, limit_marker_ttl, backing_store
= 'JetStream', is_compressed, metadata, stream}` or `nil, err`.

### kv:destroy()

Deletes the bucket stream. Returns: `true, api_msg` or `false, err`.

## Object: watcher

Returned by `kv:watch` (metatable `nats.kvs.watcher_metatable`). Fields: `kv`,
`init_done`, `received`, `last_error` (last callback error), `ordered`, `ctx`.

### watcher:wait_initial(timeout)

Pumps until the initial values were delivered. Returns: `true`, or `false` after
`timeout` seconds (default 10).

### watcher:stop() / watcher:unsubscribe()

Stops the watcher and deletes its consumer.

## Module functions

### nats.valid_bucket_name(s) / nats.valid_key_name(s)

ADR-6/8: bucket `[%w-_]+`; key `[-/_=.%w]+` without leading/trailing/double dots.
Returns: `s` or `nil`.

### nats.kvs.stream_config(cfg)

Returns: the stream config `client:kvs` would use for `cfg`, or `nil, err`.

```lua
print(nats.json.encode(nats.kvs.stream_config({ bucket = 'b', history = 3 })))
```

### nats.kvs.entry_from(kv, key, msg, headers, revision, created, delta, created_ns)

Internal: builds an [entry](#entry-table) from a message.

Module fields: `nats.kvs.metatable`, `nats.kvs.watcher_metatable`.
