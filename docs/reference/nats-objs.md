<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-objs` — Object Store

`local nats = require('nats-objs')` (loads `nats-jetstream` and `nats-sha256`;
returns `nats`). Wire compatible with nats.go (ADR-20): stream `OBJ_<bucket>`,
chunks on `$O.<bucket>.C.<nuid>`, metadata on `$O.<bucket>.M.<base64url(name)>`,
digest `SHA-256=<base64url>`.

## Client methods

### client:create_object_store(cfg)

| `cfg` field | Type | Default |
|---|---|---|
| `bucket` | string `[%w-_]+` | – (required) |
| `description` | string | – |
| `ttl` | number (s) | 0 |
| `max_bytes` | number | -1 |
| `storage` | `'file' \| 'memory'` | `'file'` |
| `replicas` | number | 1 |
| `placement`, `metadata` | table | – |
| `compression` | bool | false (`'s2'` when true) |

A string is accepted as the bucket name. Returns: [obs](#object-obs) or
`nil, 'invalid object store name' | 'stream existed on create' | err`.

```lua
local obs = assert(nc:create_object_store({ bucket = 'files', storage = 'memory' }))
```

### client:update_object_store(cfg) / client:objs(cfg)

Update an existing bucket / create or update. Returns like
`create_object_store`.

### client:object_store(bucket)

Binds to an existing bucket. Returns: obs or `nil, 'invalid object store name'
| 'object store not found' | err`.

### client:delete_object_store(bucket)

Returns: `true, api_msg` or `false, err`.

### client:object_store_names()

Returns: array of bucket names or `nil, err`.

## Object: obs

Fields: `name`, `stream`, `client`, `chunk_pre`, `meta_pre`.

### ObjectInfo table

`name`, `description`, `headers` (table key → array), `metadata`, `bucket`,
`nuid`, `size`, `chunks`, `digest`, `mtime` (RFC 3339), `deleted` (bool),
`options = {max_chunk_size, link = {bucket, name?}}`.

### obs:put(meta, data, opts)

| Param | Type | Notes |
|---|---|---|
| `meta` | string (name) or `{name, description?, headers?, metadata?, options = {max_chunk_size?}}` | `options.link` is rejected |
| `data` | string, reader `function() → chunk \| nil`, file handle (`:read`), or nil (empty object) | |
| `opts.chunk_size` | number | 128 KiB (capped at `max_payload - 512`) |
| `opts.window` | number | 32 outstanding chunk acks |
| `opts.timeout` | number | `config.timeout` |

Returns: ObjectInfo, or `nil, 'bad object meta' | 'link not allowed in put; use
add_link' | 'invalid chunk size' | 'timeout waiting for chunk acks' |
'chunk publish failed: ...' | err`. Replaces an existing object (its old chunks
are purged). **Raises** `'put: data must be a string, a reader function or a
file handle'`.

```lua
local info = assert(obs:put('a.txt', 'hello'))
obs:put({ name = 'big.bin' }, io.open('big.bin', 'rb'))
```

### obs:put_string(name, s, opts) / obs:put_bytes(name, s, opts)

`put(name, s, opts)`.

### obs:put_file(path, name, opts)

Uploads a file; `name` (string or meta table) defaults to the base name.
Returns: ObjectInfo or `nil, err`.

### obs:get(name, opts)

Reads and verifies an object (size and SHA-256 digest); follows object links
(also across buckets).

| `opts` field | Type | Default | Notes |
|---|---|---|---|
| `callback` | function(chunk) | – | stream chunks instead of collecting |
| `writer` | file handle | – | write chunks to `writer:write` |
| `batch` | number | 64 | chunks per pull |
| `timeout` | number | 5 | per pull |

Returns: `data, info` (or `true, info` when streaming), or `nil, 'object not
found' | 'cannot get a bucket link' | 'chunk count mismatch' | 'object size
mismatch' | 'object digest mismatch' | 'unsupported digest ...' | err`.

```lua
local data, info = assert(obs:get('a.txt'))
```

### obs:get_string(name, opts) / obs:get_bytes(name, opts)

`get(name, opts)`.

### obs:get_file(name, path, opts)

Writes the object to `path` (removed on failure). Returns: ObjectInfo (not the
data) or `nil, err`.

### obs:get_info(name, show_deleted)

`show_deleted` (bool or `{show_deleted = true}`) includes deleted objects.
Returns: ObjectInfo or `nil, 'object not found' | 'object name required' |
'bad object meta' | err`.

### obs:update_meta(name, meta)

Changes `name` (rename), `description`, `headers`, `metadata`. Returns:
ObjectInfo or `nil, 'cannot update meta for a deleted or missing object' |
'an object already exists with that name' | 'bad object meta' | err`.

### obs:delete(name)

Writes a deleted marker and purges the chunks. Returns: the deleted
ObjectInfo or `nil, err`.

### obs:add_link(name, target_info)

Creates a link to an object (`target_info` from `get_info`, any bucket).
Returns: link ObjectInfo or `nil, 'link name required' | 'object info required'
| 'cannot link to a deleted object' | 'cannot link to a link' | 'an object
already exists with that name' | err`.

### obs:add_bucket_link(name, other_obs)

Creates a link to another bucket. Returns like `add_link`.

### obs:list(opts)

Returns: array of ObjectInfo (`opts.show_deleted`, `opts.timeout` default 5) or
`nil, err`.

### obs:watcher(callback, opts)

Non blocking watcher on an ordered consumer. `callback(info | nil)`: `nil` once
after the initial values. `opts`: `include_history`, `ignore_deletes`,
`updates_only`. Returns: [obj_watcher](#object-obj_watcher) or `nil, err`.
Updates arrive while pumping.

```lua
local w = assert(obs:watcher(function(i) if i then print(i.name, i.deleted) end end))
w:wait_initial(5); nc:wait({ timeout = 10 }); w:stop()
```

### obs:watch(callback, opts)

**Blocking** convenience (0.2 API): runs a watcher for `opts.timeout` seconds
(default 5) or until `callback` returns `false`. Returns: `true` or
`nil, err`.

### obs:status()

Returns: `{bucket, description, metadata, ttl, storage, replicas, sealed, size,
backing_store = 'JetStream', is_compressed, stream}` or `nil, err`.

### obs:seal()

Seals the bucket (read only). Returns: `status()` or `nil, err`.

### obs:destroy()

Deletes the bucket. Returns: `true, api_msg` or `false, err`.

## Object: obj_watcher

### obj_watcher:wait_initial(timeout)

Returns: `true` once the initial values were delivered, `false` after `timeout`
(default 10).

### obj_watcher:stop() / obj_watcher:unsubscribe()

Stops the watcher.

## Module functions

### nats.valid_object_store_name(s)

Returns: `s` if it matches `[%w-_]+`, else `nil`.

### nats.objs.stream_config(cfg)

Returns: the stream config used for `cfg`.

### nats.objs.b64url_encode(s) / nats.objs.b64url_decode(s) / nats.objs.encode_name(s) / nats.objs.decode_name(s)

Padded base64url (Go `URLEncoding`) used for meta subjects; `decode` also accepts
standard alphabet / missing padding and returns `nil, 'invalid base64'` on bad
input. `encode_name`/`decode_name` are aliases.

### nats.objs.digest_value(raw)

Returns: `'SHA-256=' .. b64url_encode(raw)`.

### nats.objs.chunker(data, size)

Returns: iterator function yielding `size` byte pieces of `data`.

```lua
for c in nats.objs.chunker('abcdef', 4) do print(c) end
```

Module fields: `nats.objs.DEFAULT_CHUNK_SIZE` (131072), `nats.objs.sha256`,
`nats.objs.metatable`.
