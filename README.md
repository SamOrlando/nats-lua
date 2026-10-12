# nats-lua (lua-nats-nx)

A [NATS](https://nats.io) client written in plain Lua on top of LuaSocket. It runs on
Lua 5.1, 5.2, 5.3, 5.4 and LuaJIT. It covers core NATS (pub/sub, queue groups,
headers, request/reply, keepalive, reconnect, drain) and a practical part of
JetStream (streams, pull consumers, acks, Key-Value buckets).

The I/O model is **blocking and single threaded**: nothing happens in the
background. Messages are read and dispatched to callbacks while you call
`client:wait()`, `client:get()` / `flush()` / `drain()` or a JetStream call that
waits for a reply. Keepalive PINGs and reconnects are also handled from inside
those calls.

## Install

```sh
luarocks make rockspecs/nats-lua-0.2.0-1.rockspec
```

Requirements:

- `luasocket` (>= 3.0)
- [`nuid`](https://git.nodex.net:9002/odi/lua-nuid) >= 1.0.0 (pure Lua), used for inboxes and sids
- optional: `lua-cjson` for faster JSON. Without it, a built-in pure Lua codec is used (`nats.json.backend` tells you which one is active).
- optional: `luasec` for TLS. It is only loaded when TLS is needed.
- optional: `luaossl` (`openssl.digest`) for faster SHA-256 in the object store. Otherwise a pure Lua SHA-256 is used: native integers on 5.3+, `bit` on LuaJIT, `bit32` on 5.2, lookup tables on 5.1. Rough speed per MB: LuaJIT 0.07 s, 5.4 0.17 s, 5.3 0.26 s, 5.2 1.1 s, 5.1 4.2 s.

## Quick start

```lua
local nats = require("nats")

local nc = nats.client({ servers = { "nats://127.0.0.1:4222" }, name = "demo" })
nc:connect()

-- subscribe; the callback runs while wait()/get()/flush() pump the socket
local sid = nc:subscribe("greet.*", function(msg)
    print(msg.subject, msg.data)
    if msg.reply then msg:respond("hi " .. msg.data) end
end)

nc:publish("greet.joe", "joe")
nc:publish("greet.ann", "ann", nil, { ["X-Trace"] = "1" })   -- with headers (HPUB)
nc:wait({ maxcount = 2, timeout = 1 })                       -- dispatch 2 messages

-- request/reply (muxed inbox); returns nil, "no responders" | "timeout" on failure
local reply, err = nc:get("greet.bob", "bob")
print(reply and reply.data or err)

nc:unsubscribe(sid)
nc:drain()          -- unsubscribe all, deliver in-flight messages, close
```

The legacy forms still work: `nats.client(nil, host, port)` and
`nats.connect(host, port)`. `nats.connect` now returns the client.

### Event loop

```lua
nc:wait()                                  -- run forever, dispatching callbacks
nc:wait({ timeout = 5 })                   -- run for 5 s
nc:wait({ sid = sid, timeout = 2 })        -- until a message arrives on sid
nc:wait({ maxcount = 10, timeout = 2 })    -- until exactly 10 messages were processed
```

`wait` returns the matching message (or whatever the callback returned), or
`nil` on timeout.

### Request / reply

```lua
local m, err = nc:get(subject, data, header, nil, timeout)   -- synchronous
nc:request(subject, function(reply) ... end, data, header)   -- async, delivered from wait()
nc:poll(subject, function(reply) ... end, data, header, 2, 5)-- gather up to 5 replies within 2 s
```

All requests share one wildcard subscription, `_INBOX.<nuid>.*`, on each
connection (the "new style" muxed inbox). When nobody is listening, the server
answers with a 503 status (`no_responders`, on by default), and `get` returns
`nil, "no responders"`.

### Headers

Pass headers to `publish` either as a table (`{ Key = "v", Multi = { "a", "b" } }`)
or as an already-encoded `"Key: v\r\n"` string. On received messages,
`msg.header` is the raw header block without the `NATS/1.0` line.
`msg:headers()` returns it decoded (repeated keys become arrays), and
`msg:status()` returns the status code and description (`503`, `404`, `408`, ...).
The raw fields are `msg.status_code` and `msg.status_description`.

### Encoded connection

```lua
local ec = nc:encoded_client()               -- JSON by default (cjson or built in codec)
ec:publish("cfg.update", { level = 3 })
ec:subscribe("cfg.update", function(m) print(m.data.level) end)
```

### Reconnect, keepalive, callbacks

```lua
local nc = nats.client({
    servers = "nats://a:4222,nats://b:4222",  -- string or table; tls://, user:pass@ and token@ supported
    reconnect = true, max_reconnect = 60,     -- -1 = forever
    reconnect_wait = 2, reconnect_wait_max = 30, reconnect_jitter = 0.1,
    ping_interval = 120, max_pings_out = 2,
    error_cb = function(nc, err) end,         -- async -ERR, callback errors, disconnects
    disconnected_cb = function(nc, reason) end,
    reconnected_cb = function(nc) end,
    closed_cb = function(nc) end,
    lame_duck_cb = function(nc) end,          -- INFO with ldm=true (ADR-5)
})
-- 0.3.0: reconnects run inside the pump loop; publishes made while
-- reconnecting are buffered (reconnect_buf_size, default 8 MiB, <= 0 = old blocking reconnect)
nc:status_string()                            -- "connected" | "reconnecting" | "closed" | "disconnected"
nc:buffered()                                 -- bytes waiting in the reconnect buffer
nc:reconnect()                                -- on-demand reconnect (ADR-40, blocking)
nc:set_server_pool({ "nats://c:4222" })
```

### Auth and TLS

- user/password: set `user`/`pass`, or put them in the URL (`nats://u:p@host`).
- token: set `auth_token`, or use `nats://token@host`.
- TLS: TLS is used when the server requires it, or when you set
  `tls_required = true`, use a `tls://` URL, or set `tls_first = true`.
  `tls = {...}` takes LuaSec parameters. The default is
  `{ mode="client", protocol="any", verify="none" }`, so set
  `verify = "peer", cafile = ...` for real verification.
- nkey / creds: `creds = "/path/user.creds"` (or the creds text), `nkey_seed = "SU..."`,
  or `nkey_file = "/path/seed.nk"`. The server nonce is signed with the bundled
  pure Lua ed25519 (`nats-nkeys`, checked against RFC 8032 and nats.go vectors);
  luasodium / luatweetnacl are used when installed. `signature_cb` still overrides.
  `nats-nkeys` can also create key pairs, parse/format creds and issue user JWTs.
- Contexts: `nats.connect_context("name")` reads `~/.config/nats/context/name.json` (nats CLI).
- WebSocket: `ws://host:port/path` and `wss://` URLs (binary frames over LuaSocket,
  no permessage-deflate). `ws_headers` adds upgrade headers; `ws_mask = true` uses random masks.

### JetStream

```lua
local nats = require("nats-jetstream")       -- extends the client
local nc = nats.client(); nc:connect()
local NS = nats.jetstream.NS                  -- durations are nanoseconds

local stream = nc:stream({ name = "ORDERS", subjects = { "orders.>" }, max_age = 3600 * NS }, "create")
local ack = stream:publish("orders.new", "data", { id = "order-1" })   -- {stream=, seq=, duplicate=}
nc:js_publish("orders.new", "data")

local con = stream:consumer({ durable_name = "worker", ack_policy = "explicit" }, "create")
for _, m in ipairs(con:fetch(10, nil, 2)) do   -- batch 10, expire after 2 s
    print(m.data, m:metafields().stream_seq)
    m:ack()                                      -- also nak(delay_s), term(reason), in_progress(), ack_ack()
end
con:delete(); stream:delete()
```

### Key-Value

```lua
require("nats-kvs")
local kv = nc:kvs({ bucket = "config", history = 5, ttl = 3600 })   -- ttl in seconds
kv:put("app.level", "3")
local e = kv:get("app.level")             -- {value=, revision=, op=, created=} | nil, "key not found"
kv:update("app.level", "4", e.revision)   -- compare-and-set
kv:create("app.once", "x")                -- fails if the key exists
kv:delete("app.level"); kv:purge("app.once")
local w = kv:watch("app.*", function(e) if e then print(e.key, e.value, e.op) end end,
    { include_history = false, ignore_deletes = false, meta_only = false, updates_only = false })
w:wait_initial(5)                         -- pumps until the nil "initial values done" marker
w:stop()
kv:keys(), kv:history("app.level"), kv:purge_deletes({ older_than = 1800 })
kv:create("tmp", "x", { ttl = 10 })       -- per key TTL (bucket created with limit_marker_ttl)
nc:create_key_value({ bucket = "copy", mirror = { name = "config" } })   -- mirrors / sources
kv:destroy()
```

### Object Store (ADR-20)

```lua
require("nats-objs")
local obs = nc:create_object_store({ bucket = "files", description = "d", ttl = 0,
    storage = "file", replicas = 1, compression = true, metadata = { owner = "me" } })
-- also: nc:object_store(bucket) (bind), nc:objs(cfg) (create or update),
--       nc:update_object_store(cfg), nc:delete_object_store(bucket), nc:object_store_names()

local info = obs:put("a.txt", "hello")                                  -- string
obs:put({ name = "big.bin", description = "x", headers = { ["X-A"] = { "1" } },
          options = { max_chunk_size = 64 * 1024 } }, io.open("big.bin", "rb"))  -- file handle
obs:put("gen", function() return next_piece() end)                     -- reader function (nil = EOF)
obs:put_file("/tmp/photo.jpg")                                          -- name = base name

local data, info = obs:get("a.txt")                  -- whole object; size + SHA-256 verified
obs:get("big.bin", { callback = function(chunk) ... end })   -- streaming
obs:get_file("big.bin", "/tmp/out.bin")
obs:get_info("a.txt"), obs:get_info("a.txt", true)  -- true = include deleted
obs:update_meta("a.txt", { name = "b.txt", description = "renamed" })
obs:delete("b.txt")                                  -- deleted marker + chunk purge
obs:list(), obs:list({ show_deleted = true })
obs:add_link("alias", obs:get_info("big.bin")); obs:add_bucket_link("other", other_obs)
obs:watch(function(info) ... end, { timeout = 10 })  -- BLOCKING; nil marker after initial values
local w = obs:watcher(function(info) ... end)       -- non blocking (ordered consumer), w:stop()
obs:status(), obs:seal(), obs:destroy()
```

Chunks default to 128 KiB (capped below the server `max_payload`) and are
published with up to 32 acks outstanding. Reads use an ephemeral ack-none
pull consumer filtered on the chunk subject. Name encoding (padded base64url),
the `SHA-256=<base64url>` digest and the ObjectInfo JSON match nats.go: an
interop test (`tests/interop/`) checks that Go reads Lua objects and links and
that Lua reads Go objects and links.

## Single-threaded model

nats-lua has no threads. Everything that other clients do in the background
(reconnects, PING/PONG, consume re-pulls, idle heartbeat checks, flow control
replies, KV/object watchers, service endpoints) runs while the application
pumps the connection: `nc:wait({ timeout = t })`, `nc:get()`, `flush()`,
`request()` and the blocking JetStream calls. A long running app should loop
on `nc:wait({ timeout = 0.1 })` (or call it from its own event loop).
`nc:add_ticker(interval, fn)` runs timers inside the same loop.

### JetStream consumers (0.3.0)

```lua
local ctx = consumer:consume(function(m) m:ack() end, { max_messages = 100, expires = 30, idle_heartbeat = 5 })
ctx:stop() / ctx:drain()
local it = consumer:messages(); local m = it:next(5); it:stop()
local oc = stream:ordered_consumer({ filter_subjects = { "orders.>" } })   -- ADR-17
oc:fetch(10), oc:next(5), oc:consume(cb), oc:messages()
local sub = stream:subscribe({ deliver_subject = nc:new_inbox(), flow_control = true, idle_heartbeat = 5 }, cb)  -- push (ADR-9)
consumer:pause(os.time() + 60); consumer:resume(); consumer:unpin()                       -- ADR-42
consumer:fetch(10, nil, 5, nil, { group = "g1" })                                         -- priority groups
stream:direct_get({ multi_last = { "a.>" }, batch = 10 })                                 -- ADR-31
nc:js_publish(subj, data, { ttl = 60 })  ; nc:js_counter_add(subj, 5) ; nc:js_publish_batch(msgs)
```

### Services (ADR-32)

```lua
require("nats-micro")
local svc = nc:add_service({ name = "calc", version = "1.0.0", description = "d" })
svc:add_group("math"):add_endpoint("add", function(req) req:respond_json({ sum = 3 }) end)
nats.micro.ping(nc, "calc"); nats.micro.info(nc, "calc"); nats.micro.stats(nc, "calc")
svc:stop()
```

## Feature matrix vs. the NATS ADRs

The full per-ADR list is in [docs/ADR-COMPLIANCE.md](docs/ADR-COMPLIANCE.md).

| Area | ADR | Status |
|---|---|---|
| INFO → CONNECT → PING/PONG handshake, async INFO, discovery (+ `discovered_servers_cb`) | ADR-40 | ✅ |
| Lame duck mode notification | ADR-5 | ✅ callback + `client.lame_duck` (no automatic migration) |
| Server pool, randomization, DNS resolve-all + shuffle | ADR-40/11 | ✅ |
| Reconnect with backoff, jitter, max attempts, non-blocking in the pump loop | ADR-40 | ✅ |
| Reconnect buffer (publishes while disconnected) | ADR-40 | ✅ `reconnect_buf_size` |
| Resubscribe, callbacks (disconnected/reconnected/closed/error/lame duck) | ADR-40 | ✅ |
| Reconnect-to-specific-server handler | ADR-40 | ✅ `reconnect_delay_cb` + `set_server_pool` (no per-attempt server picker) |
| Headers, status line, header/status constants (`nats.headers`, `nats.status`) | ADR-4 | ✅ |
| No responders (503) | ADR-4/40 | ✅ |
| Subject / name validation | ADR-6 | ✅ |
| TLS, `tls_first` | ADR-40 | ✅ via LuaSec |
| nkey / JWT creds auth with bundled ed25519 | ADR-14 | ✅ |
| Contexts (nats CLI context files) | ADR-21 | ✅ |
| Request many (max messages, max wait, stall, sentinel) | ADR-47 | ✅ `request_many` |
| WebSocket transport | — | ✅ ws:// wss:// (no compression) |
| JetStream API, errors, paging, API prefix/domain | ADR-1/7/19 | ✅ |
| Streams, msg get/delete, direct get, batch / multi-last direct get | ADR-1/10/31 | ✅ |
| JetStream publish with expectations, per message TTL | ADR-22/43 | ✅ |
| Pull consumers fetch/next, status 100/404/408/409/423 | ADR-13/9 | ✅ |
| Consume / messages (simplified API), heartbeats, re-pull thresholds | ADR-37 | ✅ |
| Ordered consumer (pull and push) | ADR-17 | ✅ |
| Push consumers with flow control and idle heartbeats | ADR-9 | ✅ (application must pump) |
| Consumer pause / resume, priority groups (pinned, overflow), unpin | ADR-42 | ✅ |
| Counters (`Nats-Incr`) | ADR-49 | ✅ |
| Atomic batch publish | ADR-50 | ✅ (fast-ingest batches ❌) |
| Message scheduling | ADR-51 | ◐ header passthrough (`schedule`, `schedule_target`) |
| KV full API: watch (history, meta only, ignore deletes, updates only, resume), keys, history, purge_deletes, create/update with revision | ADR-8 | ✅ |
| KV limit markers / per key TTL, mirrors and sources | ADR-48/8 | ✅ |
| Object store (create/bind/put/get/info/meta/delete/list/links/seal/status), watcher | ADR-20 | ✅ wire compatible with nats.go |
| Service API (micro) with PING/INFO/STATS, groups, errors | ADR-32 | ✅ |
| Backup/restore, codecs, tracing | ADR-63/54/41 | ❌ |

## Client options

`nats.client(opts)` accepts these options. The same names can also be set
globally in `nats.defaults`.

| Option | Default | Notes |
|---|---|---|
| `servers` / `url` | – | string (comma separated) or table of URLs |
| `host`, `port` | 127.0.0.1, 4222 | used if no `servers` |
| `path`, `unix` | – | UNIX domain socket (LuaSocket built with unix support) |
| `name`, `user`, `pass`, `auth_token`, `jwt`, `nkey`, `sig`, `creds`, `signature_cb` | – | auth / CONNECT |
| `echo` | `true` | receive own publishes |
| `headers`, `no_responders` | `true` | |
| `verbose`, `pedantic` | `false` | CONNECT flags |
| `timeout` | 5 | seconds; request timeout and socket write timeout |
| `connect_timeout` | `timeout` | |
| `ping_interval`, `max_pings_out` | 120, 2 | |
| `reconnect`, `max_reconnect`, `reconnect_wait`, `reconnect_wait_max`, `reconnect_jitter`, `reconnect_jitter_tls`, `reconnect_delay_cb` | false, 60, 2, 30, 0.1, 1, – | |
| `no_randomize`, `ignore_discovered_servers`, `retry_on_failed_connect` | false | |
| `tls_required`, `tls_first`, `tls` | false, false, – | |
| `validate_subjects` | true | |
| `inbox_prefix` | `_INBOX` | |
| `drain_timeout` | 30 | |
| `error_cb`, `disconnected_cb`, `reconnected_cb`, `closed_cb`, `lame_duck_cb`, `discovered_servers_cb` | – | |
| `reconnect_buf_size` | 8 MiB | `<= 0` disables buffering (blocking reconnect as in 0.2) |
| `nkey_seed`, `nkey_file` | – | nkey auth with the bundled signer |
| `resolve_all` | true | try every resolved address, shuffled (ADR-11) |
| `ws_mask`, `ws_headers` | false, – | WebSocket options |

## Tests

```sh
NUID_DIR=/path/to/lua-nuid tests/run.sh                                   # unit tests on every Lua found
(cd tests/interop && go build -o interop .) && NATS_URL=... lua tests/interop/objs_interop.lua  # nats.go interop
NATS_URL=nats://127.0.0.1:4222 NUID_DIR=... tests/run.sh                  # + integration tests
NATS_NO_CJSON=1 NATS_URL=... lua tests/test_integration.lua               # with the pure Lua JSON codec
NATS_SERVER_BIN=/path/nats-server lua tests/test_local.lua                # nkey, creds, websocket, restart buffering
lua tests/test_nkeys.lua                                                  # SHA-512, ed25519 RFC 8032, nkeys vectors
lua tests/bench.lua 200000                                                # parser/publish benchmark
```

The unit tests (`tests/test_nats.lua`) drive the protocol through a fake
socket (`tests/fakesock.lua`). The integration tests only use subjects under
`nats-lua.test.<random>.>` and delete the streams and buckets they create.

## Changes in 0.3.0

New modules `nats-nkeys` and `nats-micro`. Breaking changes:

- `kv:get()` on a deleted or purged key now returns `nil, "key not found", entry`
  (before: the entry with `op = "DEL"`/`"PURGE"`).
- `kv:watch()` / `kv:watch_all()` now return a Watcher object and call the
  callback with decoded entries plus a `nil` marker after the initial values.
  Pass `{ raw = true }` to get the 0.2 behaviour (sid + raw messages, live only).
- `consumer:consume()` / `messages()` were stubs and are now the ADR-37 API.
- `list_streams()` / `consumer_list()` return all pages aggregated.
- Publishes while reconnecting are buffered instead of blocking the caller in
  a reconnect loop. Set `reconnect_buf_size = 0` for the old behaviour.
- Creds files whose JWT contains `-` were cut at the first `-`; this is fixed.

## Changes in 0.2.0

- New object store (`nats-objs`) and `nats-sha256` modules. Before, `require("nats-objs")` returned a stub table; it now returns the `nats` module, like `nats-kvs` does.

## Changes in 0.1.0 that can affect existing code

- `echo` now defaults to `true` (the NATS default). Before, a connection never
  saw its own publishes. Set `echo = false` to get the old behaviour.
- `wait({maxcount = n})` now processes exactly `n` messages. Before, it
  processed `n - 1`.
- `consumer:fetch()` / `next()` no longer JSON-decode message payloads. Use an
  encoded client or decode them yourself.
- Subscription ids are now short numeric strings instead of 22-character NUIDs
  (they are still opaque strings).
- Invalid subjects and payloads larger than `max_payload` raise an error
  before anything is sent. Before, the server disconnected the client.
- `msg.header` on received messages no longer ends with an extra blank line.
- Assigning unknown fields on client and msg objects now stores them. Before,
  a debug line was printed and the value was dropped.
- JetStream: `stream:publish(subject, data, opts)` now returns the pub ack
  table (the old version did not work). The KV `ttl` option is in seconds.

## License

MIT, Copyright (c) 2023-2026 Sam Orlando. See [LICENSE](LICENSE).
