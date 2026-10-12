<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-micro` — Service API (ADR-32)

`local nats = require('nats-micro')` returns `nats` with `nats.micro` and the
client methods below. Services answer `$SRV.PING|INFO|STATS[.<name>[.<id>]]`
with `io.nats.micro.v1.*_response` payloads (compatible with nats.go `micro`
and `nats micro`). Requests are handled while the client pumps
(`nc:wait()`), see [the pump model](nats.md#the-pump-model-threading).

## Client methods

### client:add_service(cfg) / nats.add_service(client, cfg)

| `cfg` field | Type | Default | Req | Notes |
|---|---|---|---|---|
| `name` | string `[%w-_]+` | – | yes | |
| `version` | string (semver 2.0.0) | – | yes | |
| `description` | string | `''` | no | |
| `metadata` | table string → string | `{}` | no | |
| `queue_group` | string or `false` | `'q'` | no | `false` = no queue group |
| `endpoint` | `{name?, handler, subject?, metadata?, queue_group?}` | – | no | optional root endpoint (name default `'default'`) |
| `stats_handler` | function(endpoint) → any | – | no | becomes `data` in STATS |
| `done_cb` | function(service, err) | – | no | called by `stop` |
| `error_cb` | function(service, err) | – | no | handler raised an error |

Returns: [service](#object-service-svc) or `nil, 'invalid service name' |
'invalid service version (semver required)' | <endpoint error>`.

```lua
require('nats-micro')
local svc = assert(nc:add_service({ name = 'calc', version = '1.0.0' }))
svc:add_endpoint('add', function(req)
  local t = nats.json.decode(req.data); req:respond_json({ sum = t.a + t.b })
end)
while true do nc:wait({ timeout = 0.5 }) end
```

### client:services_ping(name, id, opts) / client:services_info(name, id, opts) / client:services_stats(name, id, opts)

Methods for `nats.micro.ping/info/stats(client, ...)`.

## Object: service (`svc`)

Fields: `name`, `version`, `description`, `metadata`, `id` (nuid), `queue_group`,
`endpoints` (array), `started` (RFC 3339 ms), `stopped`, `client`.

### svc:add_endpoint(name, handler, opts)

| Param | Type | Default | Notes |
|---|---|---|---|
| `name` | string `[%w-_]+` | – | |
| `handler` | function(req) | – | runs while pumping; an error responds `500` and counts as an error |
| `opts.subject` | string | `name` | |
| `opts.metadata` | table | `{}` | |
| `opts.queue_group` | string / false | service queue group | |

Returns: endpoint table `{name, subject, queue_group, metadata, num_requests,
num_errors, last_error, processing_time, sid}` or `nil, 'service stopped' |
'handler required' | 'invalid endpoint name' | 'invalid endpoint subject'`.
(The 4th/5th parameters `prefix`, `parent_queue` are used by groups.)

### svc:add_group(name, opts)

Returns: [group](#object-group) (subject prefix `name`; `opts.queue_group`), or
`nil, 'invalid group name'`.

### svc:info() / svc:stats() / svc:ping_response()

Return the INFO / STATS / PING response tables (as served on `$SRV`). STATS
endpoints include `num_requests`, `num_errors`, `last_error`,
`processing_time` and `average_processing_time` (ns) and `data`.

### svc:reset()

Zeroes the endpoint statistics and resets `started`.

### svc:stop(err)

Unsubscribes everything, flushes (2 s), calls `done_cb(svc, err)`. Returns:
`true`.

### svc:is_stopped()

Returns: bool.

## Object: group

### group:add_endpoint(name, handler, opts)

`svc:add_endpoint` with the group's subject prefix and queue group.

### group:add_group(name, opts)

Nested group (`<prefix>.<name>`). Returns: group or `nil, err`.

```lua
local v1 = svc:add_group('calc'):add_group('v1')
v1:add_endpoint('mul', handler)          -- subject calc.v1.mul
```

## Object: request (`req`)

Passed to endpoint handlers. Message fields (`data`, `subject`, `reply`,
`header`, ...) are readable directly on `req`; also `req.msg`, `req.endpoint`,
`req.client`.

### req:respond(data, header)

Returns: `true`, or `false` when the request has no reply subject.

### req:respond_json(value, header)

`respond(nats.json.encode(value), header)`.

### req:respond_error(code, description, data, header)

Responds with `Nats-Service-Error` / `Nats-Service-Error-Code` headers (code
default 500) and counts an error. Returns like `respond`.

```lua
if not ok then return req:respond_error(400, 'bad input') end
```

### req:headers()

Returns: the request header table.

## Module functions (`nats.micro`)

### nats.micro.ping(client, name, id, opts) / nats.micro.info(client, name, id, opts) / nats.micro.stats(client, name, id, opts)

Discovery via `request_many` on `$SRV.<VERB>[.<name>[.<id>]]`.
`opts.timeout` (default 2), `opts.stall` (0.3), `opts.max` (max replies).
Returns: array of decoded responses, or `nil, 'no responders'` when no service
answered.

```lua
for _, s in ipairs(nats.micro.ping(nc, 'calc') or {}) do print(s.name, s.id) end
```

### nats.micro.response_error(msg)

Returns: `code (number), description` when `msg` is a service error response,
otherwise `nil`.

### nats.micro.error_headers(code, description)

Returns: header table for an error response.

### nats.micro.valid_name(s) / nats.micro.valid_version(v)

Returns: bool (`[%w-_]+` / semver 2.0.0).

### nats.micro.encode_body(t)

JSON-encodes a response, turning the internal empty-array placeholder into `[]`.

Constants: `API_PREFIX` (`'$SRV'`), `DEFAULT_QUEUE_GROUP` (`'q'`),
`PING_RESPONSE_TYPE`, `INFO_RESPONSE_TYPE`, `STATS_RESPONSE_TYPE`,
`ERROR_HEADER`, `ERROR_CODE_HEADER`; metatables `service_metatable`,
`group_metatable`, `request_metatable`.
