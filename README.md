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
luarocks make rockspecs/nats-lua-0.1.0-1.rockspec
```

Requirements:

- `luasocket` (>= 3.0)
- [`nuid`](https://git.nodex.net:9002/odi/lua-nuid) >= 1.0.0 (pure Lua), used for inboxes and sids
- optional: `lua-cjson` for faster JSON. Without it, a built-in pure Lua codec is used (`nats.json.backend` tells you which one is active).
- optional: `luasec` for TLS. It is only loaded when TLS is needed.

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
nc:reconnect()                                -- on-demand reconnect (ADR-40)
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
- nkey / creds: `nkey = "U..."` or `creds = "/path/user.creds"` (or the creds
  text itself). Signing the server nonce needs ed25519, which is not bundled.
  Provide `signature_cb = function(nonce, seed) return base64url_signature end`.

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
kv:watch_all(function(m) print(m.subject) end)   -- live updates only
kv:destroy()
```

## Feature matrix vs. the NATS ADRs

| Area | ADR | Status |
|---|---|---|
| INFO → CONNECT → PING/PONG handshake | ADR-40 | ✅ |
| Async INFO updates, `connect_urls` discovery | ADR-40 | ✅ (`ignore_discovered_servers`) |
| Lame duck mode notification | ADR-5 | ✅ callback + `client.lame_duck` (no automatic migration) |
| Server pool, randomization, `no_randomize` | ADR-40 | ✅ |
| Reconnect: immediate retry, exponential backoff with jitter, max attempts | ADR-40 | ✅ (blocking) |
| Resubscribe after reconnect (incl. remaining auto-unsub counts) | ADR-40 | ✅ |
| On-demand reconnect, custom server pool, reconnect delay callback | ADR-40 | ✅ |
| Reconnect-to-specific-server handler | ADR-40 | ❌ |
| Disconnect buffer (queue publishes while disconnected) | ADR-40 | ❌ publishes trigger a reconnect, then are sent |
| Retry on failed initial connect | ADR-40 | ✅ `retry_on_failed_connect` + `reconnect` |
| PING interval / max pings out | ADR-40 | ✅ only while the client is pumping (`wait`, `get`, ...) |
| Protocol `-ERR` surfaced to the app | ADR-40 | ✅ `error_cb`, `client.last_error` |
| `max_payload` enforced client side | ADR-40 | ✅ header + payload |
| Headers HPUB/HMSG, multi-value, status line | ADR-4 | ✅ case preserving, no case-insensitive helpers |
| No responders (503) | ADR-4/40 | ✅ |
| Subject / queue / name validation | ADR-6 | ✅ subjects, queues, stream/consumer/bucket/key names |
| TLS (explicit, `tls_first`) | ADR-40 | ✅ via LuaSec (not tested against a TLS server here) |
| URL options | ADR-18 | ◐ scheme, user:pass, token, host:port only |
| user/pass, token auth | — | ✅ |
| nkey / JWT creds | — | ◐ parsing done, signing needs a user `signature_cb` |
| Drain (connection and subscription) | — | ✅ `drain()`, `drain_sub()` |
| Request many | ADR-47 | ◐ `poll()` (time and count limits, no sentinel) |
| JetStream API, errors | ADR-1/7 | ✅ errors returned as `nil, msg` (msg.data.error) |
| Streams: create/update/info/delete/purge/list/names, msg get/delete | ADR-1/10/31 | ✅ |
| Direct get | ADR-31 | ✅ when `allow_direct` |
| JetStream publish with pub ack, dedupe, expectations, retries on 503 | ADR-22 | ✅ |
| Pull consumers: create/info/delete, fetch, next, no_wait, expires | ADR-13 | ✅ |
| Pull status handling 100/404/408/409 | ADR-9/13 | ✅ in fetch |
| Acks: ack, nak (delay), term, in progress, double ack | — | ✅ |
| Consume/messages (continuous pull), ordered consumer, push subscribe | ADR-15/17/37 | ❌ |
| Idle heartbeats for push consumers | ADR-9 | ❌ |
| KV: put/get/create/update/delete/purge/status/destroy, get by revision | ADR-8 | ✅ |
| KV: watch with initial values, keys, history, purge_deletes | ADR-8 | ❌ (`watch` is live updates only) |
| KV TTL per message / counters / codecs | ADR-48/49/54 | ❌ |
| Object store | ADR-20 | ❌ (`nats-objs.lua` is a stub) |
| Service API, tracing | ADR-32/41 | ❌ |
| WebSocket transport | — | ❌ |

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
| `error_cb`, `disconnected_cb`, `reconnected_cb`, `closed_cb`, `lame_duck_cb` | – | |

## Tests

```sh
NUID_DIR=/path/to/lua-nuid tests/run.sh                                   # unit tests on every Lua found
NATS_URL=nats://127.0.0.1:4222 NUID_DIR=... tests/run.sh                  # + integration tests
NATS_NO_CJSON=1 NATS_URL=... lua tests/test_integration.lua               # with the pure Lua JSON codec
lua tests/bench.lua 200000                                                # parser/publish benchmark
```

The unit tests (`tests/test_nats.lua`) drive the protocol through a fake
socket (`tests/fakesock.lua`). The integration tests only use subjects under
`nats-lua.test.<random>.>` and delete the streams and buckets they create.

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
