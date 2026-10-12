<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-jetstream` — streams, consumers, acks

`local nats = require('nats-jetstream')` returns the `nats` module with JetStream
methods added to the client, msg, stream and consumer objects.

Error convention: JetStream API errors are returned as `nil, msg` where
`msg.data.error` is the server's `{code, err_code, description}` (ADR-1/7);
transport problems are `nil, 'timeout'` or `nil, 'JetStream not enabled (no
responders)'`. Some functions use `false, err` instead of `nil, err` (noted per
function). Durations given to this module are **seconds**; JetStream config
tables you pass through (stream/consumer config) use the server's native units
(nanoseconds) — `nats.jetstream.NS` (1e9) helps convert.

All waiting happens through the client's pump loop; continuous consumers
(`consume`, `messages`, ordered and push consumers) only make progress while the
application calls `nc:wait()` or another blocking call. See
[the pump model](nats.md#the-pump-model-threading).

## Contents

- [Client methods](#client-methods)
- [Object: stream](#object-stream)
- [Object: consumer](#object-consumer)
- [Object: consume context (`ctx`)](#object-consume-context-ctx)
- [Object: message iterator (`iter`)](#object-message-iterator-iter)
- [Object: ordered consumer](#object-ordered-consumer)
- [Object: push subscription (`push_sub`)](#object-push-subscription-push_sub)
- [Object: msg (JetStream methods)](#object-msg-jetstream-methods)
- [Module functions](#module-functions)

## Client methods

### client:stream(cfg, opts, action)

Gets, creates or updates a stream and returns a cached stream object.

| Param | Type | Default | Req | Notes |
|---|---|---|---|---|
| `cfg` | table (stream config) or string (name) | – | yes | a name alone means `action = 'info'` |
| `opts` | table, or the action string | `{}` | no | `domain` (→ `$JS.<domain>.API.`), `pre` (API prefix, e.g. `'$JS.API.'`) — ADR-19 |
| `action` | `'info' \| 'create' \| 'update' \| nil` | nil = create or update | no | |

Returns: `stream, api_msg`. Errors: `nil, 'no stream name provided'`,
`nil, 'invalid stream name'`, `nil, 'stream not found', api_msg` (info/update),
`nil, 'stream existed on create'` (when already cached or found), `nil, api_msg`
(server error), `nil, 'timeout'`. ADR-1, ADR-33/35/36/43/60 config fields pass
through unchanged.

```lua
local st = assert(nc:stream({ name = 'ORDERS', subjects = { 'orders.>' }, storage = 'memory' }))
```

### client:get_stream(name, opts) / client:create_stream(cfg, opts) / client:update_stream(cfg, opts) / client:create_update_stream(cfg, opts)

`client:stream` with the action `'info'`, `'create'`, `'update'`, nil.

### client:delete_stream(name, opts)

Returns: `true, api_msg` or `false, err`.

```lua
nc:delete_stream('ORDERS')
```

### client:stream_names(subject, opts)

All pages of `STREAM.NAMES`, optionally filtered by `subject`. `opts`: `domain`,
`pre`, `api_prefix`. Returns: array of names, or `nil, err`.

### client:stream_by_subject(subject)

Returns: the first stream name capturing `subject`, `nil` if none, or `nil, err`.

### client:list_streams(subject, opts)

All pages of `STREAM.LIST`. Returns: `{ streams = {stream info...}, total = n }`
or `false, err`.

### client:account_info(opts)

Returns: the decoded `$JS.API.INFO` response (limits, usage, `api.level`, ...) or
`nil, err`.

```lua
local ai = assert(nc:account_info()); print(ai.streams, ai.memory)
```

### client:js_publish(subject, data, opts)

Publishes and waits for the pub ack. Retries on no responders (ADR-22).

| `opts` field | Type | Header / meaning |
|---|---|---|
| `header` | table/string | extra headers |
| `id` | string | `Nats-Msg-Id` (dedupe) |
| `last_msg_id` | string | `Nats-Expected-Last-Msg-Id` |
| `stream` | string | `Nats-Expected-Stream` |
| `last_seq` | number | `Nats-Expected-Last-Sequence` |
| `last_subject_seq` | number | `Nats-Expected-Last-Subject-Sequence` |
| `last_subject_seq_subject` | string | `Nats-Expected-Last-Subject-Sequence-Subject` |
| `ttl` | number (s) or string (`'1h'`, `'never'`) | `Nats-TTL` (ADR-43) |
| `incr` | number/string | `Nats-Incr` (ADR-49) |
| `schedule`, `schedule_target` | string | `Nats-Schedule`, `Nats-Schedule-Target` (ADR-51, passthrough) |
| `required_api_level` | number | `Nats-Required-Api-Level` |
| `timeout` | number | default `config.timeout` |
| `retry_attempts` | number | 2 |
| `retry_wait` | number | 0.25 |

Returns: `ack, reply_msg` (`ack = {stream, seq, duplicate?, domain?, val?}`);
`nil, 'invalid pub ack'`; `nil, description, ack` for a server error;
`nil, 'timeout' | 'no responders'`. Raises like `client:publish`.

```lua
local ack = assert(nc:js_publish('orders.new', '{}', { id = 'o-1', ttl = 60 }))
print(ack.seq)
```

### client:js_counter_add(subject, delta, opts)

Adds `delta` (default 1) to a counter stream subject (`allow_msg_counter`).
ADR-49. `opts` as `js_publish`. Returns: `value (number), ack` or `nil, err`.

```lua
local v = assert(nc:js_counter_add('hits.page1', 5))
```

### client:js_publish_batch(msgs, opts)

Atomic batch publish (ADR-50; stream needs `allow_atomic = true`). `msgs` is an
array of `{subject, data, header?, ttl?, msg_id?}`. The first and last messages
are requests; the last carries `Nats-Batch-Commit: 1`.

| `opts` field | Type | Default |
|---|---|---|
| `id` | string | new nuid |
| `ack_every` | number | – (also request every n-th message) |
| `timeout` | number | `config.timeout` |

Returns: `ack, reply_msg` (`ack.batch`, `ack.count`, `ack.seq`); `nil, 'empty
batch'`; `nil, description, ack`; `nil, 'empty reply to batch commit'`;
`nil, 'timeout'`.

```lua
local ack = assert(nc:js_publish_batch({ { subject = 'b.1', data = 'x' }, { subject = 'b.2', data = 'y' } }))
```

## Object: stream

Fields: `name`, `config` (stream config as returned by the server), `state`,
`cluster`, `created`, `ts`, `api` (API prefix), `opts`, `client`,
`consumers` (cache name → consumer).

### stream:info()

Refreshes from the server. Returns: `stream, api_msg` or `nil, err`.

### stream:update(cfg, opts)

Updates with `cfg` (default the current `stream.config`). Returns like
`client:stream`.

### stream:delete()

Returns: `true, api_msg` or `false, err`.

### stream:purge(seq, subject, keep)

ADR-10 purge. Forms: `purge()`, `purge(subject)`, `purge(subject, keep)`,
`purge(seq, subject, keep)` (`seq`: purge up to, not including, this sequence).
Returns: `true, api_msg` or `false, err`.

```lua
st:purge('orders.old', 1) -- keep the last message on that subject
```

### stream:getmsg(seq, subject)

Gets one message. Forms: `getmsg(seq)`, `getmsg(subject)` (last on subject),
`getmsg(seq, subject)` (next on subject at or after `seq` when `seq >= 1`).
With `allow_direct` uses direct get (ADR-31) and returns the raw message (headers
`Nats-Stream`, `Nats-Sequence`, `Nats-Subject`, `Nats-Time-Stamp`); otherwise
`STREAM.MSG.GET` and returns the API msg (`msg.data.message` holds the base64
payload). Errors: `nil, 'message not found', status_msg`, `nil, err`.

```lua
local m = assert(st:getmsg('orders.new')); print(m:headers()['Nats-Sequence'])
```

### stream:lastmsg(subject) / stream:nextmsg(subject)

`getmsg(0, subject)` / `getmsg(nil, subject)`. Note: both currently return the
**last** message on `subject` (see API notes in the index).

### stream:deletemsg(seq, secure)

Deletes a message; `secure = true` overwrites (erases) it. Returns: `true,
api_msg` or `false, err`.

### stream:publish(subject, data, opts) / stream:publish(msg, opts)

`client:js_publish` with `opts.stream` defaulting to this stream name
(expected stream check). A msg object supplies subject/data/header.

### stream:direct_get(req, timeout)

Batch / multi-last direct get (ADR-31; `allow_direct` required).

| `req` field | Type | Notes |
|---|---|---|
| `seq`, `next_by_subj`, `last_by_subj` | number / string | single message forms |
| `batch`, `max_bytes` | number | batch from `seq` / `next_by_subj` |
| `multi_last` | array of subjects (wildcards ok) | last message per subject |
| `up_to_seq`, `up_to_time` | number / RFC 3339 | multi_last snapshot |
| `start_time` | RFC 3339 | |

Returns: `msgs, eob` (`eob = {num_pending, last_seq, up_to_seq}` from the 204
end-of-batch marker), or `nil, 'message not found' | err`.

```lua
local msgs = assert(st:direct_get({ multi_last = { 'orders.>' } }))
```

### stream:counter_add(subject, delta)

`client:js_counter_add(subject, delta)` (opts are not passed).

### stream:counter_get(subject)

Returns: `value (number), msg` from the last counter message, or `nil, err`
(`'invalid counter value'`). ADR-49.

### stream:consumer(cfg, opts, action)

Gets, creates or updates a consumer and returns a cached consumer object.

| Param | Type | Default | Notes |
|---|---|---|---|
| `cfg` | table (consumer config) or string (name) | – | `durable = true \| '<name>'` is a legacy alias for `durable_name`; a missing name gets a random one |
| `opts` | table, or the action string | – | currently unused |
| `action` | `'info' \| 'create' \| 'update' \| nil` | nil = create or update | sent as the API `action` |

Returns: `consumer, api_msg`. Errors: `nil, 'invalid durable name'`,
`nil, 'invalid consumer name'`, `nil, 'consumer not found', api_msg`,
`nil, api_msg`, `nil, 'timeout'`. ADR-13/34/42 config fields pass through.

```lua
local c = assert(st:consumer({ name = 'work', ack_policy = 'explicit' }, 'create'))
```

### stream:get_consumer(name) / stream:create_consumer(cfg) / stream:update_consumer(cfg) / stream:create_update_consumer(cfg)

`stream:consumer` with action `'info'`, `'create'`, `'update'`, nil.

### stream:delete_consumer(name)

Returns: `true, api_msg` or `false, err`.

### stream:consumer_list()

All pages. Returns: `{ consumers = {...}, total = n }` or `false, err`.

### stream:consumer_names()

Returns: array of names or `nil, err`.

### stream:pause_consumer(name, until_t) / stream:resume_consumer(name)

ADR-42 pause. `until_t`: unix seconds or RFC 3339 string; default one hour from
now. Returns: the decoded response (`{paused, pause_until, pause_remaining}`) or
`nil, err`.

### stream:unpin_consumer(name, group)

Unpins the pinned client of priority group `group` (ADR-42). Returns:
`true, api_msg` or `nil, err`.

### stream:ordered_consumer(cfg)

Creates an [ordered consumer](#object-ordered-consumer) (no server call until
first use). ADR-17.

| `cfg` field | Type | Default |
|---|---|---|
| `filter_subject` / `filter_subjects` | string / array | all |
| `deliver_policy` | string | `'all'` |
| `opt_start_seq`, `opt_start_time` | number / RFC 3339 | |
| `replay_policy` | string | `'instant'` |
| `headers_only` | bool | |
| `inactive_threshold` | number (s) | 300 |
| `max_reset_attempts` | number | unlimited (-1/nil) |
| `name_prefix` | string | random |
| `metadata` | table | |
| `reset_cb` | function(ordered, reason) | |

```lua
local oc = st:ordered_consumer({ filter_subjects = { 'orders.>' } })
for _, m in ipairs(oc:fetch(10, 2)) do print(m.data) end
```

### stream:subscribe(cfg, callback, opts)

Push consumer (ADR-9), optionally ordered (ADR-17). `subscribe(callback, opts)`
is also accepted.

| Param | Type | Notes |
|---|---|---|
| `cfg` | consumer config | `deliver_subject` defaults to a new inbox |
| `callback` | function(msg) | user messages only; heartbeats / flow control are handled internally |
| `opts.idle_heartbeat` | number (s) | default 5 when flow control or ordered |
| `opts.flow_control` | bool | answer `FlowControl Request` status messages |
| `opts.ordered` | bool | ordered push consumer with gap detection and reset |
| `opts.queue` | string | `deliver_group` + queue subscription |
| `opts.action` | string | consumer create action |
| `opts.error_cb` | function(push_sub, err) | status messages, callback errors, `'missed idle heartbeat'` |
| `opts.keep` | bool | do not delete an ephemeral consumer on `stop` |

Returns: [push_sub](#object-push-subscription-push_sub) or `nil, err`
(`'callback required'`, consumer errors). Two missed heartbeats raise
`error_cb(sub, 'missed idle heartbeat')` (and reset an ordered consumer).

```lua
local sub = assert(st:subscribe({ ack_policy = 'explicit' }, function(m) m:ack() end, { flow_control = true }))
```

## Object: consumer

Fields: `name`, `durable_name`, `durable`, `config`, `stream`, `delivered`,
`ack_floor`, `num_ack_pending`, `num_redelivered`, `num_waiting`, `num_pending`,
`paused`, `pause_remaining`, `cluster`, `ts`, `sid` (private pull inbox
subscription), `pin_id` (ADR-42).

### consumer:info()

Returns: `consumer, api_msg` (refreshed) or `nil, err`.

### consumer:update(cfg, opts)

Updates with `cfg` (name defaults to this consumer). Returns like
`stream:consumer`.

### consumer:delete()

Returns: `true, api_msg` or `false, err`.

### consumer:fetch(n, max_bytes, timeout, heartbeat, opts)

One pull request (ADR-13).

| Param | Type | Default | Notes |
|---|---|---|---|
| `n` | number | 1 | batch size |
| `max_bytes` | number | – | |
| `timeout` | number (s) | – | `> 0` → `expires`; nil/`<= 0` → `no_wait` |
| `heartbeat` | number (s) | – | `idle_heartbeat` |
| `opts.group` | string | – | ADR-42 priority group |
| `opts.min_pending`, `opts.min_ack_pending`, `opts.priority` | number | – | overflow / prioritized policies |
| `opts.id` | string | `consumer.pin_id` | pinned client id |

Returns: `msgs` (array, possibly short), or `{}, status_description` when the
first reply is a status (404 no messages, 408 timeout, 409, 423 pin mismatch —
which also clears `pin_id`), or `{}, 'consumer inbox not subscribed'`.
Heartbeats (100) are skipped. Payloads are raw.

```lua
local msgs = c:fetch(10, nil, 2)
for _, m in ipairs(msgs) do m:ack() end
```

### consumer:next(timeout, heartbeat)

Returns: `msg, err` (`fetch(1, nil, timeout, heartbeat)`).

### consumer:consume(callback, opts)

Continuous pull (ADR-37). Returns a [consume context](#object-consume-context-ctx)
or `nil, 'callback required'`.

| `opts` field | Type | Default |
|---|---|---|
| `max_messages` | number | 500 (1,000,000 when only `max_bytes`) |
| `max_bytes` | number | – |
| `expires` | number (s, min 1) | 30 |
| `idle_heartbeat` | number (s) | `min(expires/2, 30)` |
| `threshold_messages` | number | 50% of `max_messages` |
| `threshold_bytes` | number | 50% of `max_bytes` |
| `error_cb` | function(ctx, err, status) | status/heartbeat/callback errors |
| `group`, `min_pending`, `min_ack_pending`, `priority` | | ADR-42 priority groups |
| `pinned_cb` | function(ctx, pin_id) | the server pinned this client |

`callback(msg)` runs while pumping. Pull requests are reissued when the pending
count drops to the threshold, after 404/408/409, after 2 missed heartbeats, and
after a reconnect. 409 "consumer deleted" / "consumer is push based" stop the
context.

```lua
local ctx = c:consume(function(m) process(m); m:ack() end, { max_messages = 100 })
while running do nc:wait({ timeout = 0.5 }) end
ctx:stop()
```

### consumer:messages(opts)

Iterator form of `consume` (same `opts`). Returns: [iter](#object-message-iterator-iter)
or `nil, err`.

```lua
local it = c:messages()
local m = it:next(5); if m then m:ack() end
it:stop()
```

### consumer:pause(until_t) / consumer:resume() / consumer:unpin(group)

`stream:pause_consumer` / `resume_consumer` / `unpin_consumer` for this consumer.

### consumer:stop() / consumer:drain()

Placeholders kept for API symmetry. Always return `nil, 'use the context
returned by consume():stop()'` (or `drain()`).

## Object: consume context (`ctx`)

Returned by `consumer:consume()` (metatable `nats.jetstream.consume_context`).
Fields: `consumer`, `client`, `running`, `delivered`, `pulls`, `missed`,
`pending_msgs`, `pending_bytes`, `last_error`, `opts`.

### ctx:stop()

Stops pulling, unsubscribes, removes the ticker and reconnect hook. Messages of
outstanding pull requests are dropped (they are redelivered after `ack_wait`).

### ctx:drain(timeout)

Stops issuing pulls, delivers what is in flight for up to `timeout` seconds
(default 5), then `stop()`.

### ctx:closed()

Returns: `true` once stopped.

Internal (exported, not for applications): `ctx:pull()`, `ctx:on_msg(m)`,
`ctx:check(now)`, `ctx:reset_pending()`.

## Object: message iterator (`iter`)

Returned by `consumer:messages()` and `ordered:messages()`.

### iter:next(timeout)

Pumps until a message is queued. Returns: `msg`, or `nil, 'timeout'` (after
`timeout` seconds, default 30) / `nil, 'stopped'`.

### iter:stop() / iter:drain()

Stops the underlying context (`drain` is an alias of `stop`).

## Object: ordered consumer

Returned by `stream:ordered_consumer()` (metatable
`nats.jetstream.ordered_metatable`). Fields: `stream`, `cfg`, `consumer`
(current server consumer), `stream_seq`, `consumer_seq`, `resets`, `serial`.
The consumer is ephemeral, `ack_policy = 'none'`, memory storage, one replica,
and is recreated from `stream_seq + 1` after a gap, a missing consumer or missed
heartbeats. ADR-17.

### ordered:fetch(n, timeout)

Creates the consumer if needed and fetches up to `n` messages (timeout default
5 s). Returns: `msgs, err` (`err` only when no message was returned).

### ordered:next(timeout)

Returns: `msg, err`.

### ordered:consume(callback, opts)

Like `consumer:consume` (same `opts`) with automatic resets. Returns:
[ordered_ctx](#object-ordered-consume-context-ordered_ctx) or `nil, err`.
`opts.error_cb(ordered_ctx, err)` is called when a reset fails (the context then
stops).

### ordered:messages(opts)

Returns: [iter](#object-message-iterator-iter) or `nil, err`.

### ordered:reset(reason)

Deletes the current consumer and creates a new one from the next sequence.
Returns: `consumer` or `nil, err` (`'ordered consumer: too many resets'`).
Calls `cfg.reset_cb(ordered, reason)`.

### ordered:delete_current() / ordered:stop()

Forget and delete (best effort, no wait) the current server consumer.

### ordered:info()

Returns: `consumer:info()` of the current consumer, or nothing.

Internal: `ordered:config()`, `ordered:create()`, `ordered:track(m)`.

### Object: ordered consume context (`ordered_ctx`)

Returned by `ordered:consume()`. Fields of the active `ctx` are readable through
it (`delivered`, `pulls`, ...); `ordered`, `client`, `last_error` are its own.

#### ordered_ctx:stop()

Stops consuming and deletes the current consumer. Call as `octx.stop()` or
`octx:stop()` (it ignores its argument).

#### ordered_ctx:closed()

Returns: `true` once stopped (also after a failed reset).

## Object: push subscription (`push_sub`)

Returned by `stream:subscribe()`. Fields: `consumer`, `deliver`, `sid`, `hb`,
`running`, `missed`, `fc_replies`, `stalls`, `ordered`.

### push_sub:stop() / push_sub:unsubscribe()

Unsubscribe, remove the heartbeat ticker and delete the consumer (ephemeral
consumers, unless `opts.keep`; ordered consumers always).

### push_sub:reset(reason)

Ordered push only: recreate the consumer. Errors go to `opts.error_cb`.

## Object: msg (JetStream methods)

Added to every [msg](nats.md#object-msg). Ack methods need `msg.reply`
(JetStream deliveries).

### msg:ack(t, opt, ack)

Sends an acknowledgement (default `+ACK`). With `ack = true` waits for the
server's confirmation (`client:get`). Returns: the published msg (or the reply
msg with `ack = true`), or `false, 'Message already acknowledged.'` /
`false, 'ErrMsgNoReply'`.

### msg:ack_ack() / msg:double_ack()

`+ACK` and wait for the server confirmation. Returns: `reply_msg` or
`nil, 'timeout'`, or `false, reason`.

### msg:nak(delay)

`-NAK`, optionally with a redelivery `delay` in seconds.

### msg:term(reason)

`+TERM`, optionally with a reason string.

### msg:in_progress() / msg:wpi()

`+WPI` (resets the ack timer; can be sent repeatedly).

### msg:ack_next()

`+NXT` (ack and request the next message), sent as a request.

### msg:acked()

Returns: `true` once a terminal ack was sent by this process.

### msg:pending()

Returns: `pending_messages, pending_bytes` from the status headers (or nils).

### msg:metafields()

Parses the `$JS.ACK...` reply subject. Returns: `{stream, consumer, delivered,
stream_seq, consumer_seq, timestamp (ns), pending, domain?, account_hash?, token?}`
or `{}`.

```lua
local meta = m:metafields(); print(meta.stream_seq, meta.pending)
```

## Module functions

### nats.valid_stream_name(s) / nats.valid_consumer_name(s)

ADR-6: 1..255 characters without `. * > / \` or whitespace/control characters.
Returns: `s` or `nil`.

### nats.jetstream.publish(client, subject, data, opts)

Function form of `client:js_publish`.

### nats.jetstream.ttl_string(v)

Returns: `Nats-TTL` value — numbers are rounded up to whole seconds, strings
returned as is.

### nats.jetstream.incr_string(v)

Returns: `Nats-Incr` value with an explicit sign (`5` → `'+5'`).

### nats.jetstream.api(client, subject, payload)

Low level JetStream API request. Returns: `msg` with `msg.data` decoded, or
`nil, msg` (API error), `nil, 'invalid JetStream API response'`,
`nil, 'JetStream not enabled (no responders)'`, `nil, 'timeout'`.

```lua
local m = assert(nats.jetstream.api(nc, '$JS.API.INFO'))
```

### nats.jetstream.api_prefix(opts)

Returns: the API prefix for `opts` (`domain` → `$JS.<domain>.API.`, `pre`,
`api_prefix`, default `$JS.API.`).

### nats.jetstream.paged(client, subject, field, req)

Collects `data[field]` over all pages of a list API. Returns: array or
`nil, err`.

### nats.jetstream.priority_fields(consumer, req, opts)

Internal helper: copies ADR-42 fields from `opts` into a pull request `req`.
Returns: `req`.

### Module fields

`nats.jetstream.NS` (1e9), `stream_metatable`, `consumer_metatable`,
`consume_context`, `ordered_metatable`, `objs` (alias of `nats.objs` after
`require 'nats-objs'`).
