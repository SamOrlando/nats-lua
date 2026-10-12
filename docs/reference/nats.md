<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats` — core client

`local nats = require('nats')` (file `nats.lua`, v0.3.0). Depends on LuaSocket and
`nuid`; LuaSec is loaded only for TLS, lua-cjson is used when installed.

Conventions used in this reference:

- **Raises** means the function calls `error()` (through `client.error`, which is
  `nats.error` by default). Wrap in `pcall` if you need to recover.
- **Returns `nil, err`** means a soft failure; `err` is a string unless noted.
- Times are **seconds** (numbers, fractions allowed) unless noted.
- Every blocking call pumps the connection; see [the pump model](#the-pump-model-threading).

## Contents

- [The pump model (threading)](#the-pump-model-threading)
- [Module functions](#module-functions)
- [Client options](#client-options)
- [Object: client](#object-client)
- [Object: msg](#object-msg)
- [Object: encoded client](#object-encoded-client-eclient)
- [Constants: `nats.headers`, `nats.status`](#constants)
- [JSON codec `nats.json`](#json-codec)
- [Low level / internal helpers](#low-level--internal-helpers)

## The pump model (threading)

nats-lua is single threaded and uses blocking LuaSocket I/O. Nothing runs in the
background: incoming messages, subscription callbacks, PING/PONG keepalive,
reconnect attempts, flushing the reconnect buffer, tickers
(`client:add_ticker`), JetStream re-pulls and heartbeat checks, flow control
replies, KV/object watchers and service endpoints all run **only while the
application is inside a pumping call**:

| Pumping call | Pumps until |
|---|---|
| `client:wait(opts)` | timeout, a matching sid/lid, or `maxcount` messages |
| `client:get()`, `request_many()`, `flush()`, `drain()` | reply / PONG / deadline |
| JetStream / KV / object store blocking calls (`fetch`, `put`, `get`, `keys`, ...) | their reply |
| `iter:next()`, `watcher:wait_initial()`, `obs:watch()` | data / deadline |

A long running program loops on `nc:wait({ timeout = 0.1 })` (or calls it from its
own event loop). Callbacks run inside that call, on the same stack: a callback
that blocks stalls everything else on the connection. Errors raised by callbacks
are caught and reported to `error_cb` (and `client.last_error`).

```lua
local nats = require('nats')
local nc = nats.connect({ servers = 'nats://127.0.0.1:4222', reconnect = true })
nc:subscribe('greet.*', function(m) m:respond('hi ' .. m.subject) end)
nc:add_ticker(5, function() print('stats', nc.stats.in_msgs) end)
while true do nc:wait({ timeout = 0.1 }) end
```

**Reconnect while not pumping**: a network failure is noticed by the next read or
write. With `reconnect = true` and `reconnect_buf_size > 0` (default 8 MiB) the
client enters the *reconnecting* state, publishes are buffered, and attempts are
made from the pump loop (non blocking for the caller). With
`reconnect_buf_size <= 0` the failing call blocks in the reconnect loop (0.2
behaviour). Control lines (SUB/UNSUB/PING) are not buffered; subscriptions are
restored from the client's subscription table after reconnecting.

## Module functions

### nats.client(opts) / nats.client(host, port, timeout)

Creates a client object without connecting.

| Param | Type | Default | Req | Notes |
|---|---|---|---|---|
| `opts` | table | `{}` | no | [client options](#client-options); unknown keys are silently dropped (see `nats.valid_config`) |
| `host` | string | `nats.defaults.host` (`127.0.0.1`) | no | legacy positional form (when the first arg is a string) |
| `port` | number | `nats.defaults.port` (4222) | no | |
| `timeout` | number | `nats.defaults.tcp_timeout` or -1 | no | stored in `client.network.timeout` |

Returns: client. Raises: nothing. Example:

```lua
local nc = nats.client({ servers = { 'nats://a:4222', 'nats://b:4222' }, name = 'svc' })
nc:connect()
```

### nats.connect(opts) / nats.connect(host, port, timeout)

`nats.client(...)` followed by `client:connect()`.
Returns: connected client. **Raises** if no server could be reached (unless
`retry_on_failed_connect` + `reconnect` are set, in which case it blocks in the
reconnect loop). ADR-40.

```lua
local nc = nats.connect({ url = 'nats://127.0.0.1:4222' })
```

### nats.connect_context(name, extra)

Loads a nats CLI context (`nats.load_context(name)`), merges `extra` (table of
client options, overriding the context) and calls `nats.connect`. ADR-21.

| Param | Type | Default | Req |
|---|---|---|---|
| `name` | string | selected context (`context.txt`) | no; a value containing `/` is a file path |
| `extra` | table | `{}` | no |

Returns: connected client, or `nil, err` if the context cannot be loaded.
**Raises** on connection failure (from `nats.connect`).

```lua
local nc = assert(nats.connect_context('prod', { name = 'worker' }))
```

### nats.load_context(name)

Reads `<context_dir>/context/<name>.json` (or the file `name` if it contains a
`/`; or the context named in `<context_dir>/context.txt` when `name` is nil/empty).
ADR-21.

Returns: `opts, raw` — a client options table (`servers`, `user`, `pass`,
`auth_token`, `creds`, `nkey_file`, `inbox_prefix`, `tls`, `tls_required`,
`tls_first`, and `jetstream = {domain, api_prefix, event_prefix}`) and the decoded
context JSON; or `nil, err` (`'no context selected'`, `'context not found: <path>'`,
`'invalid context json'`). `~` in paths is expanded with `$HOME`.

```lua
local opts = assert(nats.load_context())
print(opts.servers)
```

### nats.context_dir()

Returns: string — `$XDG_CONFIG_HOME/nats`, or `$HOME/.config/nats`.

```lua
print(nats.context_dir())
```

### nats.create_inbox()

Legacy module level inbox, not bound to a client's muxed request subscription.
Returns: `subject, lid` (`_INBOX.<22 chars>.<8 chars>`, and the 8 char id).
Prefer `client:create_inbox()` / `client:new_inbox()`.

```lua
local inbox = nats.create_inbox()
```

### nats.msg(subject, data, reply, header, client, sid)

Constructs a [msg](#object-msg) object (no I/O). All parameters optional.
Returns: msg.

```lua
local m = nats.msg('a.b', 'payload', nil, { ['X-A'] = '1' })
```

### nats.encode_header(t)

Encodes a header table to `"Key: value\r\n..."` lines. Array values produce one
line per value. A string argument is returned unchanged. Returns: string. ADR-4.

```lua
print(nats.encode_header({ ['X-A'] = { '1', '2' } })) -- "X-A: 1\r\nX-A: 2\r\n"
```

### nats.decode_header(s)

Decodes header lines (optionally starting with the `NATS/1.0 <code> <desc>`
status line) into a table. Repeated keys become arrays; a status line sets
`Status` and `Description`. Non-string input is returned unchanged.
Returns: table. ADR-4.

```lua
local h = nats.decode_header('NATS/1.0 503\r\nX-A: 1\r\n') -- {Status='503', ['X-A']='1'}
```

### nats.valid_subject(s) / nats.valid_publish_subject(s) / nats.valid_queue(q)

ADR-6 checks. `valid_subject` allows wildcards (`*`, `>` as whole tokens, `>`
last); `valid_publish_subject` rejects wildcards; `valid_queue` rejects empty
names and whitespace. Returns: the input string when valid, otherwise `nil`.

```lua
assert(nats.valid_subject('a.*.>')); assert(not nats.valid_publish_subject('a.*'))
```

### nats.timestamp(t)

Returns: RFC 3339 UTC string (`YYYY-MM-DDTHH:MM:SSZ`) for unix seconds `t`
(default `os.time()`).

```lua
print(nats.timestamp())
```

### nats.parse_url(u, default_port)

Parses a server URL (`nats://`, `tls://`, `ws://`, `wss://`, `user:pass@`,
`token@`, `[ipv6]:port`, ws path). A table is returned unchanged.
Returns: server table `{url, host, port, attempts, tls?, ws?, ws_path?, user?, pass?, token?}`.
Default ports: 4222, ws 80, wss 443 (or `default_port`). ADR-40.

```lua
local s = nats.parse_url('wss://u:p@example.com/nats') -- host, port=443, ws_path='/nats', tls=true
```

### nats.read_creds(creds)

Reads a NATS creds file (path, or the creds text itself when it contains
`-----BEGIN`). Returns: `jwt, seed` (seed may be nil), or `nil, err`
(`'cannot read creds file: ...'`, `'creds: user JWT not found'`).

```lua
local jwt, seed = assert(nats.read_creds(os.getenv('HOME') .. '/user.creds'))
```

### nats.error(message, level)

Default error function (`client.error`): `error(message, level + 1)`. Always
raises. Replace `client.error` to customise.

## Client options

`nats.client(opts)` keeps only the keys listed in `nats.valid_config`. Any key
can also be given a process wide default in `nats.defaults` (e.g.
`nats.defaults.timeout = 2`).

| Option | Type | Default | Notes |
|---|---|---|---|
| `servers` / `url` | string (comma/space separated) or array | – | server pool; `nats://`, `tls://`, `ws://`, `wss://` |
| `host`, `port` | string, number | `127.0.0.1`, 4222 | used when no `servers`/`url` |
| `unix`, `path` | bool, string | – | UNIX socket at `path` (LuaSocket with unix support) |
| `name` | string | – | connection name (CONNECT) |
| `user`, `pass`, `auth_token` | string | – | also from the URL |
| `creds` | string | – | creds file path or text; signs the nonce with the built-in ed25519 (ADR-14) |
| `nkey_seed`, `nkey_file` | string | – | nkey auth with a seed / seed file |
| `nkey`, `jwt`, `sig` | string | – | raw CONNECT fields; `nkey` starting with `S` is treated as a seed |
| `signature_cb` | function(nonce, seed) → base64url sig | – | overrides built-in signing |
| `echo`, `headers`, `no_responders` | bool | true | CONNECT flags |
| `verbose`, `pedantic` | bool | false | CONNECT flags |
| `protocol` | number | 1 | |
| `timeout` | number | 5 | request timeout and socket write timeout |
| `connect_timeout` | number | `timeout` | |
| `tcp_nodelay` | bool | true | |
| `ping_interval`, `max_pings_out` | number | 120, 2 | keepalive, only while pumping |
| `reconnect` | bool | false | enable automatic reconnect |
| `max_reconnect` | number | 60 | attempts per server; -1 = forever |
| `reconnect_wait`, `reconnect_wait_max` | number | 2, 30 | exponential backoff base/cap |
| `reconnect_jitter`, `reconnect_jitter_tls` | number | 0.1, 1 | |
| `reconnect_delay_cb` | function(attempt) → seconds | – | replaces the backoff |
| `reconnect_buf_size` | number (bytes) | 8 MiB | publish buffer while reconnecting; `<= 0` = blocking reconnect |
| `retry_on_failed_connect` | bool | false | with `reconnect`, retry the initial connect |
| `no_randomize` | bool | false | keep pool and DNS order |
| `ignore_discovered_servers` | bool | false | ignore `connect_urls` |
| `resolve_all` | bool | true | try every resolved address, shuffled (ADR-11) |
| `tls_required`, `tls_first` | bool | false | |
| `tls` | table | `{mode='client', protocol='any', options='all', verify='none'}` | LuaSec params (merged) |
| `ws_headers` | table | – | extra HTTP headers for the WebSocket upgrade |
| `ws_mask` | bool | false | random frame masks (otherwise zero mask key) |
| `validate_subjects` | bool | true | ADR-6 checks on publish/subscribe |
| `inbox_prefix` | string | `_INBOX` | prefix of the muxed request inbox |
| `drain_timeout` | number | 30 | |
| `max_chan_len` | number | 65536 | stored, currently unused |
| `error_cb` | function(client, err) | – | async `-ERR`, callback errors, disconnects |
| `disconnected_cb` | function(client, reason) | – | entering reconnect |
| `reconnected_cb` | function(client) | – | after subscriptions + buffer were restored |
| `closed_cb` | function(client) | – | `close()` or reconnect gave up |
| `lame_duck_cb` | function(client) | – | INFO with `ldm` (ADR-5) |
| `discovered_servers_cb` | function(client) | – | new `connect_urls` were added |

## Object: client

Created by `nats.client` / `nats.connect`. Methods are looked up through
`nats.client_metatable.methods`, which the other modules extend (JetStream, KV,
object store, services — see their reference pages).

Fields (read only unless noted):

| Field | Type | Notes |
|---|---|---|
| `connected`, `closed`, `reconnecting` | bool | |
| `config` | table | effective options |
| `network` | table | host/port/path/timeout used when no URL is configured |
| `server_information` | table | last INFO (`server_id`, `version`, `max_payload`, `jetstream`, `nonce`, ...) |
| `servers` | array | server pool entries (`parse_url` tables) |
| `current_server` | table | |
| `subscriptions` | table sid → `{subject, queue, callback, delivered, max}` | |
| `stats` | table | `in_msgs, out_msgs, in_bytes, out_bytes, reconnects` |
| `lame_duck` | bool | ADR-5 |
| `last_error` | string/false | last reported async error |
| `inbox_prefix`, `inbox_sid` | string | muxed request subscription |
| `error` | function | error raiser (writable) |
| `streams` | table | JetStream stream object cache (after `require 'nats-jetstream'`) |

### client:connect()

Connects to the first reachable server of the pool (INFO → TLS → CONNECT/PING →
PONG), then (re)subscribes. Returns: `true`. **Raises** the last connection error
if every server fails (unless `retry_on_failed_connect` + `reconnect`, which
blocks in the reconnect loop and raises when it gives up). No-op when connected.
ADR-40.

```lua
local nc = nats.client({ url = 'nats://127.0.0.1:4222' }); nc:connect()
```

### client:reconnect()

On-demand reconnect (ADR-40): closes the socket and **blocks** until reconnected.
Fires `disconnected_cb`, then `reconnected_cb`. Returns: `true`. **Raises**
`'connection error: reconnect failed (...)'` when `max_reconnect` is exhausted
(the client is then closed and `closed_cb` fires).

```lua
nc:reconnect()
```

### client:set_server_pool(servers)

Replaces the server pool (`servers`: string or array of URLs). Returns: the new
pool array. Takes effect on the next (re)connect. ADR-40.

```lua
nc:set_server_pool({ 'nats://c:4222' })
```

### client:is_connected() / client:is_reconnecting()

Returns: bool.

### client:status_string()

Returns: `'connected' | 'reconnecting' | 'closed' | 'disconnected'` (ADR-40).

```lua
print(nc:status_string())
```

### client:buffered()

Returns: number of bytes of publishes held in the reconnect buffer (0 when
connected).

### client:publish(subject, data, reply, header)

Sends PUB/HPUB.

| Param | Type | Default | Req | Notes |
|---|---|---|---|---|
| `subject` | string | – | yes | no wildcards (validated when `validate_subjects`) |
| `data` | string (other values are `tostring`ed) | `''` | no | |
| `reply` | string | – | no | |
| `header` | table or string | – | no | table → `encode_header`; a string without `NATS/` gets the version line added |

Returns: the published [msg](#object-msg) object. While reconnecting the
message is buffered. **Raises**: `'invalid subject: ...'`, `'invalid reply
subject: ...'`, `'headers are disabled on this connection'`, `'maximum payload
exceeded: ...'` (header + data > server `max_payload`), `'reconnect buffer
exceeded (...)'`, `'not connected'` / `'connection error: ...'` when reconnect is
off. ADR-4, ADR-40.

```lua
nc:publish('orders.new', '{"id":1}', nil, { ['Nats-Msg-Id'] = '1' })
```

### client:subscribe(subject, callback, queue)

| Param | Type | Default | Req | Notes |
|---|---|---|---|---|
| `subject` | string | – | yes | wildcards allowed |
| `callback` | function(msg) → any | passthrough | no | runs while pumping; a non-nil return value ends a matching `wait` and is returned by it |
| `queue` | string | – | no | queue group |

Returns: `sid` (string). **Raises** on invalid subject/queue.

```lua
local sid = nc:subscribe('orders.>', function(m) print(m.subject, m.data) end, 'workers')
```

### client:unsubscribe(sid, max)

Without `max`: UNSUB and forget the subscription. With `max > 0`: the server
auto-unsubscribes after `max` messages in total (the count survives reconnects).
Returns: nothing.

```lua
nc:unsubscribe(sid, 10)
```

### client:request(subject, callback, data, header, sid)

Asynchronous request. Publishes with a reply subject on the client's muxed inbox
(or on the subject of subscription `sid` when given). The `callback(msg)` runs
for the first reply while pumping.
Returns: the published msg with `msg.sid` and `msg.lid` (inbox token) set, which
can be passed to `wait({lid = ...})`. Raises like `publish`.

```lua
local r = nc:request('time', function(m) print(m.data) end)
nc:wait({ lid = r.lid, timeout = 2 })
```

### client:get(subject, data, header, sid, timeout) / client:request_sync(...)

Synchronous request (`request_sync` is an alias).

| Param | Type | Default | Req |
|---|---|---|---|
| `subject` | string | – | yes |
| `data` | string | `''` | no |
| `header` | table/string | – | no |
| `sid` | string | muxed inbox | no |
| `timeout` | number | `config.timeout` | no |

Returns: reply msg; `nil, 'timeout'`; or `nil, 'no responders', status_msg`
(503, ADR-4). Raises like `publish`.

```lua
local m, err = nc:get('svc.echo', 'hi', nil, nil, 2)
```

### client:poll(subject, callback, data, header, wait, count)

Scatter request: `callback(msg)` runs for every reply within `wait` seconds
(default 2) or until `count` replies. Non blocking; replies arrive while
pumping. Returns: published msg (`lid` set). Prefer `request_many`. ADR-47.

```lua
nc:poll('$SRV.PING', function(m) print(m.data) end, '', nil, 1)
nc:wait({ timeout = 1 })
```

### client:request_many(subject, data, opts)

Blocking scatter/gather (ADR-47).

| `opts` field | Type | Default | Notes |
|---|---|---|---|
| `header` | table/string | – | |
| `max_messages` | number | unlimited | |
| `max_wait` | number | `config.timeout` | total time |
| `stall` | number | – | end when no new reply for this long (after the first) |
| `sentinel` | function(msg) → bool, or `true` | – | `true`: an empty payload without headers ends; the sentinel itself is not collected |
| `callback` | function(msg) → `false` ends | – | replies are not collected when set |

Returns: array of msgs (possibly empty); `{}, 'no responders'` when the only
reply was a 503. Raises like `publish`.

```lua
local replies = nc:request_many('$SRV.PING', '', { max_wait = 1, stall = 0.2 })
```

### client:wait(opts)

The event loop: reads and dispatches protocol operations.

| `opts` field | Type | Default | Notes |
|---|---|---|---|
| `timeout` | number (or `{n}` / numeric string) | none (blocks forever) | |
| `sid` | string | – | return when a message for this subscription arrives |
| `lid` | string | – | return when the reply for this request token arrives |
| `maxcount` | number | – | return after this many messages |

Returns: the matching msg (or the callback's non-nil return value), the last msg
once `maxcount` messages were processed, or `nil` on timeout. **Raises**
`'connection error: ...'` when the connection fails and reconnect is off, or
when reconnect gives up.

```lua
while running do nc:wait({ timeout = 0.25 }) end
```

### client:flush(timeout)

Sends PING and pumps until the PONG. While reconnecting, first waits (pumping
reconnect attempts) for the connection. Returns: `true`, or `false` on timeout
(default `config.timeout`).

```lua
assert(nc:flush(2))
```

### client:ping(timeout) / client:pong()

`ping` is `flush`. `pong` writes a raw PONG (normally answered automatically).

### client:drain_sub(sid, timeout)

UNSUB, process what is in flight (one `flush`), then remove the subscription.
Returns: `true`/`false` (`false` also when `sid` is unknown).

### client:drain(timeout)

Graceful shutdown: unsubscribe everything, flush, wait for pending request
replies, then `close()`. `timeout` defaults to `drain_timeout` (30).
Returns: `true` if everything completed, else `false`.

```lua
nc:drain(5)
```

### client:close() / client:shutdown()

Close the socket, stop reconnecting, fire `closed_cb` (once). `shutdown` calls
`socket:shutdown()` first. Returns: nothing. Subsequent I/O raises
`'not connected'`.

### client:create_inbox()

Returns: `subject, lid` — a fresh subject under this client's muxed request
subscription (`<inbox_prefix>.<nuid>`); replies to it are routed to
`wait({lid=})` / request callbacks registered in `client.inbox_table`.

### client:new_inbox()

Returns: `_INBOX.<nuid>`, a subject for your own `subscribe` (not routed through
the muxed inbox). Note: always uses `_INBOX`, not `config.inbox_prefix`.

```lua
local inbox = nc:new_inbox(); nc:subscribe(inbox, print)
```

### client:inbox_subscribe()

Returns: the sid of the muxed request subscription (created on connect).

### client:auth(user, pass)

Sets `config.user`/`config.pass` for the next CONNECT. Returns: `{user, pass}`.

### client:msg(subject, data, reply, header, sid)

Returns: a new msg bound to this client (no I/O).

### client:add_ticker(interval, fn)

Registers a timer run from the pump loop. `fn(client, now)`; returning `false`
removes it; errors are reported to `error_cb`. Tickers also cap the read timeout
so they fire on time while `wait` blocks. Returns: the ticker handle (table).

```lua
local t = nc:add_ticker(1, function(c, now) print('tick', now) end)
nc:remove_ticker(t)
```

### client:remove_ticker(t)

Removes a ticker handle. Returns: nothing.

### client:on_reconnect(fn) / client:off_reconnect(fn)

Internal hook list used by JetStream contexts: `fn(client)` runs after every
successful reconnect, before `reconnected_cb`. Returns: `fn` (on) / nothing (off).

### client:encoded_client(encode_data, decode_data, encode_header, decode_header)

Wraps the client so payloads/headers are encoded on send and decoded on
receive. Defaults: JSON (`nats.json`) for data, `nats.encode_header` /
`nats.decode_header` for headers. Returns: [encoded client](#object-encoded-client-eclient).

```lua
local ec = nc:encoded_client()
ec:publish('cfg', { level = 3 })
```

## Object: msg

Received messages, published messages (returned by `publish`) and `nats.msg()`
results. Metatable `nats.msg_metatable`; JetStream adds ack methods (see
[nats-jetstream](nats-jetstream.md#object-msg-jetstream-methods)).

| Field | Type | Notes |
|---|---|---|
| `subject`, `reply`, `data` | string | `data` is raw bytes (decoded value on an encoded client) |
| `header` | string (raw header lines, without version line) or table | use `m:headers()` |
| `sid` | string | subscription id |
| `lid` | string | request token (replies on the muxed inbox) |
| `status_code`, `status_description` | string | status messages (e.g. `'503'`, `'100'`) |
| `client` | client | |

### msg:headers()

Returns: header table (decoded on demand; `{}` when none). ADR-4.

```lua
local h = m:headers(); print(h['Nats-Msg-Id'])
```

### msg:status()

Returns: `code (number), description` for status messages, or `nil`.

### msg:respond(data, reply, header)

Publishes `data` to `m.reply`. Returns: the published msg, or `nil` when the
message has no reply subject.

```lua
nc:subscribe('echo', function(m) m:respond(m.data) end)
```

### msg:publish(subject, data, reply, header)

Re-publishes, each argument defaulting to the message's own field. Returns:
published msg.

### msg:request(subject, callback, data, header, sid) / msg:get(subject, data, header, sid)

`client:request` / `client:get` with arguments defaulting to the message's
fields.

### msg:new(subject, data, reply, header, sid)

Returns: a new msg bound to the same client.

### msg:clone(subject, data, reply, header, sid)

Returns: a copy. Note the asymmetry: `subject`/`sid` arguments override the
message's values, while `data`/`reply`/`header` arguments are only used when
the message's own field is nil.

### msg:size()

Returns: number — `#subject + #reply + #header + #data` (encoded sizes on an
encoded client).

## Object: encoded client (`eclient`)

Returned by `client:encoded_client()`. Unknown keys and methods fall through to
the wrapped client (`ec.client`), so every client method works.

### eclient:publish(subject, data, reply, header)

Like `client:publish` with `data` / `header` encoded first.

### eclient:subscribe(subject, callback, queue)

`callback(msg)` receives a msg whose `data`/`header` are decoded and whose
`client` is the encoded client.

### eclient:request(subject, callback, data, header, sid) / eclient:get(subject, data, header, sid, timeout) / eclient:poll(subject, callback, data, header, wait, count)

Encoded versions of the client calls; replies are decoded.
`get` returns `msg, err, raw` like `client:get`.

```lua
local ec = nc:encoded_client()
local r = ec:get('cfg.get', { key = 'a' }); print(r.data.value)
```

### eclient:encode_data(v) / eclient:decode_data(s) / eclient:encode_header(h) / eclient:decode_header(s)

Run the configured codecs. Errors are reported to `error_cb` and return `nil`.
`decode_data('')` returns `nil`.

## Constants

### `nats.headers`

| Key | Header |
|---|---|
| `MSG_ID` | `Nats-Msg-Id` |
| `EXPECTED_STREAM` | `Nats-Expected-Stream` |
| `EXPECTED_LAST_SEQ` | `Nats-Expected-Last-Sequence` |
| `EXPECTED_LAST_SUBJECT_SEQ` | `Nats-Expected-Last-Subject-Sequence` |
| `EXPECTED_LAST_SUBJECT_SEQ_SUBJECT` | `Nats-Expected-Last-Subject-Sequence-Subject` |
| `EXPECTED_LAST_MSG_ID` | `Nats-Expected-Last-Msg-Id` |
| `ROLLUP` | `Nats-Rollup` |
| `STREAM`, `SEQUENCE`, `TIMESTAMP`, `SUBJECT`, `LAST_SEQUENCE` | `Nats-Stream`, `Nats-Sequence`, `Nats-Time-Stamp`, `Nats-Subject`, `Nats-Last-Sequence` |
| `NUM_PENDING`, `UP_TO_SEQUENCE` | `Nats-Num-Pending`, `Nats-UpTo-Sequence` (direct get batches, ADR-31) |
| `PENDING_MESSAGES`, `PENDING_BYTES` | `Nats-Pending-Messages`, `Nats-Pending-Bytes` (pull status) |
| `LAST_CONSUMER`, `LAST_STREAM`, `CONSUMER_STALLED` | `Nats-Last-Consumer`, `Nats-Last-Stream`, `Nats-Consumer-Stalled` (heartbeats, ADR-9) |
| `TTL`, `MARKER_REASON` | `Nats-TTL` (ADR-43), `Nats-Marker-Reason` (ADR-48) |
| `INCR` | `Nats-Incr` (ADR-49) |
| `BATCH_ID`, `BATCH_SEQUENCE`, `BATCH_COMMIT` | `Nats-Batch-Id`, `Nats-Batch-Sequence`, `Nats-Batch-Commit` (ADR-50) |
| `SCHEDULE`, `SCHEDULE_TARGET` | `Nats-Schedule`, `Nats-Schedule-Target` (ADR-51) |
| `REQUIRED_API_LEVEL` | `Nats-Required-Api-Level` |
| `SERVICE_ERROR`, `SERVICE_ERROR_CODE` | `Nats-Service-Error`, `Nats-Service-Error-Code` (ADR-32) |
| `KV_OPERATION` | `KV-Operation` |
| `STATUS`, `DESCRIPTION` | `Status`, `Description` (keys produced by `decode_header`) |

### `nats.status`

| Key | Code | Meaning |
|---|---|---|
| `IDLE_HEARTBEAT` | 100 | idle heartbeat / flow control request |
| `EOB` | 204 | end of a direct get batch |
| `BAD_REQUEST` | 400 | |
| `NOT_FOUND` | 404 | no messages (pull) / message not found |
| `TIMEOUT` | 408 | pull request expired |
| `CONFLICT` | 409 | max waiting, consumer deleted, leadership change, ... |
| `PIN_ID_MISMATCH` | 423 | priority group pin mismatch (ADR-42) |
| `NO_RESPONDERS` | 503 | |

Note: `msg.status_code` is a string (`'503'`); compare with
`tonumber(m.status_code) == nats.status.NO_RESPONDERS` or use `m:status()`.

### Other module fields

`nats._VERSION` (`'lua-nats-nx 0.3.0'`), `nats._DESCRIPTION`, `nats._COPYRIGHT`,
`nats.verbose` (bool, prints protocol traffic), `nats.defaults` (table),
`nats.valid_config` (set of accepted option names), `nats.nuid` (the `nuid`
module), `nats.client_metatable` (`lang`, `version`, `methods`),
`nats.encoded_client_metatable`, `nats.msg_metatable`.

## JSON codec

`nats.json` is lua-cjson when installed, otherwise a built-in pure Lua codec.
Fields: `backend` (`'cjson'` | `'lua'`), `null` (JSON null sentinel).

### nats.json.encode(v)

Returns: JSON string. **Raises** on unsupported types (functions, NaN/inf).
Empty tables encode as `{}`.

### nats.json.decode(s)

Returns: Lua value (`null` → `nil` in the pure Lua codec, `cjson.null` with
cjson). **Raises** on invalid JSON.

```lua
local t = nats.json.decode(nats.json.encode({ a = 1 }))
```

## Low level / internal helpers

Exported for tests, transports and other modules; their signatures may change.

### nats.socket_factory(srv, cfg)

Creates and connects the raw socket for a pool entry (`srv` from `parse_url`):
UNIX socket, or TCP to every resolved address (ADR-11, shuffled unless
`no_randomize`), wrapped in WebSocket for `ws`. Returns: socket-like object, or
`nil, err`. Overridable (tests replace it with a fake socket).

### nats.websocket_wrap(sock, srv, cfg, timeout)

Performs the WebSocket upgrade on a connected socket (TLS first for `wss`) and
returns an object with `send/receive/settimeout/close/dirty` (binary frames), or
`nil, err` (`'websocket upgrade refused: ...'`, ...). Ping frames are answered.

### nats.ws_frame(op, data, mask)

Returns: one final WebSocket frame string (opcode `op`, client mask: random when
`mask` is true, zero key otherwise).

### nats.tls_wrap(sock, cfg, host, timeout)

Wraps a socket with LuaSec using `cfg.tls`, sets SNI, performs the handshake.
Returns: TLS socket, or `nil, err` (`'TLS requested but LuaSec (ssl) is not installed'`, ...).

### nats.msg_string(client, subject, data, reply, header)

Returns: the PUB/HPUB protocol string. **Raises** `'maximum payload exceeded'`.

### nats.msg_stringtable(subject, data, reply, header)

Legacy: `{ msg_string(...) }` without payload limit.

### nats.split_header(block)

Splits a received header block. Returns: `header_lines|nil, status|nil, description|nil`.

### nats.response_read(client, timeout)

Reads one protocol operation. Returns: op table (`{op='MSG'|'HMSG', subject, sid,
reply, data, header, status, description}`, `{op='INFO', data}`, `{op='-ERR',
data}`, PING/PONG/+OK singletons) or `nil, 'timeout' | err`.
