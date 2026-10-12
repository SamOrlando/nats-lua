-- nats-jetstream.lua - JetStream streams, consumers and acks for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Streams (ADR-1/10/31/33/35/36/43/50/60), pull consumers (ADR-13), continuous
-- pull "consume" (ADR-37), ordered consumers (ADR-17/37), push consumers with
-- flow control and idle heartbeats (ADR-9), consumer pause and priority groups
-- (ADR-42), direct get incl. batch/multi_last (ADR-31), per message TTL (ADR-43),
-- counters (ADR-49) and atomic batch publish (ADR-50).
--
-- Continuous consumers are driven by the client's pump loop: messages are
-- delivered, pull requests re-issued and heartbeats checked while the
-- application is in client:wait() (or any other blocking call).

-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = nats.jetstream or {}

-- ### Local properties ###
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber, tostring, pcall = tonumber, tostring, pcall
local pairs, type = pairs, type
local floor, ceil, min, max = math.floor, math.ceil, math.min, math.max
local ipairs = ipairs
local format, gmatch, find = string.format, string.gmatch, string.find
local insert, concat = table.insert, table.concat
local encode, decode = nats.json.encode, nats.json.decode
local encode_header, decode_header = nats.encode_header, nats.decode_header
local gettime, sleep = require('socket').gettime, require('socket').sleep

local NS = 1000000000 -- JetStream durations are nanoseconds
nats.jetstream.NS = NS

-- copy go vars
local defaultAPIPrefix = "$JS.API."
local jsDomainT = "$JS.%s.API."

-- quick local references and functions
local cm = nats.client_metatable.methods -- client meta methods
local mm = nats.msg_metatable.methods -- msg meta methods
local valid_subject = nats.valid_subject
local function unwrap(c) return rawget(c, 'client') or c end

-- ADR-6: stream/consumer names are "filename-safe": printable, no . * > / \ or whitespace
function nats.valid_consumer_name(s)
	if type(s) ~= 'string' or #s == 0 or #s > 255 or find(s, "[>*. /\\%c%s]") then return nil end
	return s
end
local valid_consumer_name = nats.valid_consumer_name
nats.valid_stream_name = nats.valid_consumer_name
local valid_stream_name = nats.valid_stream_name

-- JetStream API request: returns msg with decoded data, or nil, err (err may be the msg with .data.error)
local function api(client, subject, payload)
	local m, err = cm.get(unwrap(client), subject, payload)
	if not m then
		if err == 'no responders' then return nil, 'JetStream not enabled (no responders)' end
		return nil, err
	end
	local ok, d = pcall(decode, (m.data and m.data ~= '') and m.data or '{}')
	if not ok or type(d) ~= 'table' then return nil, 'invalid JetStream API response' end
	rawset(m, 'data', d)
	if d.error then return nil, m end
	return m
end
nats.jetstream.api = api

--
-- Streams
local stream_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] end,
	methods = {},
}
nats.jetstream.stream_metatable = stream_metatable
local js = stream_metatable.methods -- stream meta methods

local function stream_update(stream, data, opts, client)
	opts = opts or {}
	setmetatable(stream, nil)
	stream.api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre
		or stream.api
		or defaultAPIPrefix
	stream.state = data.state or stream.state or {}
	stream.cluster = data.cluster or stream.cluster or {}
	stream.client = client or stream.client
	stream.consumers = stream.consumers or {}
	stream.config = data.config or stream.config or {}
	stream.name = data.name or stream.config.name or stream.name
	stream.created = data.created or stream.created
	stream.opts = {
		pre = opts.pre,
		wait = opts.wait,
		domain = opts.domain,
	}
	stream.ts = data.ts or stream.ts
	setmetatable(stream, stream_metatable)
	stream.client.streams[stream.name] = stream
	return stream
end

local function stream_create(client, data, opts) return stream_update({}, data, opts, client) end

-- client:stream(cfg|name [, opts] [, action]) action = 'info' | 'create' | 'update' | nil (create or update)
function cm.stream(client, scfg, opts, action)
	if not scfg then return nil, 'no stream name provided' end
	if not action then
		if type(opts) == 'string' then
			action = opts
			opts = nil
		end
		if type(scfg) == 'string' then
			scfg = { name = scfg }
			action = action or 'info'
		end
	elseif type(scfg) == 'string' then
		scfg = { name = scfg }
	end
	opts = type(opts) == 'table' and opts or {}

	if not valid_stream_name(scfg.name) then return nil, 'invalid stream name' end

	client = unwrap(client)
	if not rawget(client, "streams") then rawset(client, "streams", {}) end

	local stream_api = opts.domain and format(jsDomainT, opts.domain) or opts.pre or defaultAPIPrefix

	local stream = client.streams[scfg.name]
	local msg, err
	if not stream or action == 'info' then
		msg, err = api(client, (stream and stream.api or stream_api) .. 'STREAM.INFO.' .. scfg.name)
		if msg then
			stream = stream and stream_update(stream, msg.data, opts, client) or stream_create(client, msg.data, opts)
		else
			if type(err) ~= 'table' then return nil, err end -- timeout / JetStream not available
			if action == 'info' or action == 'update' then return nil, 'stream not found', err end
			stream = nil
		end
		if action == 'info' then return stream, msg end
	end

	if stream then
		if action == 'create' then return nil, 'stream existed on create' end
		msg, err = api(client, stream.api .. 'STREAM.UPDATE.' .. stream.name, encode(scfg))
		if not msg then return nil, err end
		stream = stream_update(stream, msg.data, opts, client)
	else
		msg, err = api(client, stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
		if not msg then return nil, err end
		stream = stream_create(client, msg.data, opts)
	end
	return stream, msg
end

-- per StreamManager interface request
function cm.get_stream(client, name, opts) return cm.stream(client, name, opts, 'info') end
function cm.create_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'create') end
function cm.update_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'update') end
function cm.create_update_stream(client, scfg, opts) return cm.stream(client, scfg, opts) end
function cm.delete_stream(client, name, opts)
	local stream, err = cm.stream(client, name, opts, 'info')
	if not stream then return false, err end
	return js.delete(stream)
end
-- paged list helper: collects data[field] over all pages (offset/total/limit)
local function paged(client, subject, field, req)
	local out, offset = {}, 0
	while true do
		local r = {}
		for k, v in pairs(req or {}) do r[k] = v end
		r.offset = offset
		local m, err = api(client, subject, encode(r))
		if not m then return nil, err end
		local items = m.data[field]
		if type(items) ~= 'table' then break end
		for _, v in ipairs(items) do out[#out + 1] = v end
		offset = offset + #items
		local total = tonumber(m.data.total) or 0
		if #items == 0 or offset >= total then break end
	end
	return out
end
nats.jetstream.paged = paged

local function api_prefix(opts)
	opts = opts or {}
	return opts.domain and format(jsDomainT, opts.domain) or opts.pre or opts.api_prefix or defaultAPIPrefix
end
nats.jetstream.api_prefix = api_prefix

function cm.stream_names(client, subject, opts)
	return paged(client, api_prefix(opts) .. 'STREAM.NAMES', 'streams', subject and { subject = subject } or nil)
end
function cm.stream_by_subject(client, subject)
	local names, err = cm.stream_names(client, subject)
	if not names then return nil, err end
	return names[1]
end
-- TODO paging support for information and not objects
function cm.list_streams(client, subject, opts)
	local l, err = paged(client, api_prefix(opts) .. "STREAM.LIST", 'streams', subject and { subject = subject } or nil)
	if not l then return false, err end
	return { streams = l, total = #l }
end
function cm.account_info(client, opts)
	local m, err = api(client, api_prefix(opts) .. "INFO")
	if not m then return nil, err end
	return m.data
end

function js.update(stream, scfg, opts) -- update server and update obj
	return cm.stream(stream.client, scfg or stream.config, opts, 'update')
end

function js.info(stream) -- get info from server
	return cm.stream(stream.client, stream.name, nil, 'info')
end

function js.delete(stream)
	local msg, err = api(stream.client, stream.api .. 'STREAM.DELETE.' .. stream.name)
	if not msg then return false, err end
	stream.client.streams[stream.name] = nil
	return true, msg
end

-- purge([seq] [, subject] [, keep])  (ADR-10)
function js.purge(stream, seq, subject, keep)
	if type(seq) == 'string' and not keep then subject, keep, seq = seq, subject, nil end
	local req = { filter = subject, seq = seq, keep = keep }
	local msg, err = api(stream.client, stream.api .. 'STREAM.PURGE.' .. stream.name, next(req) and encode(req) or nil)
	if not msg then return false, err end
	return true, msg
end

-- getmsg(seq [, subject]) / getmsg(subject): direct get when allowed (ADR-31)
-- returns the raw message (headers Nats-Sequence, Nats-Subject, ...) or nil, err
function js.getmsg(stream, seq, subject)
	if type(seq) == 'string' and not subject then
		subject = seq
		seq = nil
	end
	local data = { seq = seq }
	if subject then
		if data.seq and data.seq >= 1 then
			data.next_by_subj = subject
		else
			data.last_by_subj = subject
			data.seq = nil
		end
	end
	local client = stream.client
	local msg, err
	if stream.config.allow_direct then
		if data.last_by_subj then
			msg, err = cm.get(client, stream.api .. format('DIRECT.GET.%s.%s', stream.name, subject))
		else
			msg, err = cm.get(client, stream.api .. format('DIRECT.GET.%s', stream.name), encode(data))
		end
		local st = msg and rawget(msg, 'status_code')
		if st then
			return nil, st == '404' and 'message not found' or (rawget(msg, 'status_description') or st), msg
		end
		return msg, err
	end
	msg, err = api(client, stream.api .. format('STREAM.MSG.GET.%s', stream.name), encode(data))
	if not msg then return nil, err end
	return msg
end

function js.lastmsg(stream, subject) return js.getmsg(stream, 0, subject) end
function js.nextmsg(stream, subject) return js.getmsg(stream, nil, subject) end

function js.deletemsg(stream, seq, secure)
	local data = secure == true and { seq = seq } or { seq = seq, no_erase = true }
	local msg, err = api(stream.client, stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, encode(data))
	if not msg then return false, err end
	return true, msg
end

-- JetStream publish: returns pub ack table {stream=, seq=, duplicate=} and the reply msg, or nil, err
-- opts: id, last_msg_id, stream, last_seq, last_subject_seq, header (table), timeout,
--       retry_attempts (default 2), retry_wait (seconds, default 0.25)   (ADR-22)
-- ADR-43 TTL: number = seconds, string passed as is ('1h', 'never')
function nats.jetstream.ttl_string(v)
	if type(v) == 'number' then return format('%d', ceil(v)) end
	return tostring(v)
end
function nats.jetstream.incr_string(v)
	if type(v) == 'number' then return (v >= 0 and '+' or '') .. format('%.0f', v) end
	v = tostring(v)
	if not find(v, '^[+-]') then v = '+' .. v end
	return v
end

local function js_publish(client, subject, data, opts)
	opts = type(opts) == 'table' and opts or {}
	local header = opts.header and decode_header(opts.header) or {}
	if type(header) ~= 'table' then header = {} end
	if opts.id then header['Nats-Msg-Id'] = opts.id end
	if opts.last_msg_id then header['Nats-Expected-Last-Msg-Id'] = opts.last_msg_id end
	if opts.stream then header['Nats-Expected-Stream'] = opts.stream end
	if opts.last_seq then header['Nats-Expected-Last-Sequence'] = tostring(opts.last_seq) end
	if opts.last_subject_seq then header['Nats-Expected-Last-Subject-Sequence'] = tostring(opts.last_subject_seq) end
	if opts.last_subject_seq_subject then
		header['Nats-Expected-Last-Subject-Sequence-Subject'] = opts.last_subject_seq_subject
	end
	if opts.ttl then header['Nats-TTL'] = nats.jetstream.ttl_string(opts.ttl) end -- ADR-43
	if opts.incr then header['Nats-Incr'] = nats.jetstream.incr_string(opts.incr) end -- ADR-49
	if opts.schedule then header['Nats-Schedule'] = opts.schedule end -- ADR-51
	if opts.schedule_target then header['Nats-Schedule-Target'] = opts.schedule_target end
	if opts.required_api_level then header['Nats-Required-Api-Level'] = tostring(opts.required_api_level) end
	local hdr = next(header) and encode_header(header) or nil
	local attempts = opts.retry_attempts or 2
	local m, err
	for i = 0, attempts do
		m, err = cm.get(unwrap(client), subject, data, hdr, nil, opts.timeout)
		if m or err ~= 'no responders' then break end
		if i < attempts then sleep(opts.retry_wait or 0.25) end
	end
	if not m then return nil, err end
	local ok, ack = pcall(decode, m.data)
	if not ok or type(ack) ~= 'table' then return nil, 'invalid pub ack' end
	if ack.error then return nil, ack.error.description or 'publish error', ack end
	return ack, m
end
nats.jetstream.publish = js_publish
function cm.js_publish(client, subject, data, opts) return js_publish(client, subject, data, opts) end

-- stream:publish(subject, data [, opts]) or stream:publish(msg [, opts]); expects this stream
function js.publish(stream, subject, data, opts)
	if type(subject) == 'table' then -- msg object
		local m = subject
		opts = data
		subject, data = m.subject, m.data
		opts = type(opts) == 'table' and opts or {}
		opts.header = opts.header or m.header
	end
	opts = type(opts) == 'table' and opts or {}
	if opts.stream == nil then opts.stream = stream.name end
	return js_publish(stream.client, subject, data, opts)
end

--
-- Consumers
local consumer_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] end,
	methods = {},
}
nats.jetstream.consumer_metatable = consumer_metatable
local con = consumer_metatable.methods -- consumer meta methods

local consumer_nuid = nats.nuid({ preLen = 8, seqLen = 0 })

local function consumer_update(consumer, data, stream)
	setmetatable(consumer, nil)
	local cfg = data.config or consumer.config or {}
	consumer.config = cfg
	consumer.durable_name = cfg.durable_name or consumer.durable_name
	consumer.durable = cfg.durable_name and true or consumer.durable or false
	consumer.name = data.name or cfg.name or consumer.name
	consumer.stream = stream or consumer.stream
	consumer.delivered = data.delivered or consumer.delivered
	consumer.ack_floor = data.ack_floor or consumer.ack_floor
	consumer.num_ack_pending = data.num_ack_pending or consumer.num_ack_pending
	consumer.num_redelivered = data.num_redelivered or consumer.num_redelivered
	consumer.num_waiting = data.num_waiting or consumer.num_waiting
	consumer.num_pending = data.num_pending or consumer.num_pending
	consumer.paused = data.paused or consumer.paused
	consumer.pause_remaining = data.pause_remaining or consumer.pause_remaining
	consumer.cluster = data.cluster or consumer.cluster
	consumer.ts = data.ts or consumer.ts
	-- unique pull inbox for this consumer object (not shared across clients/processes)
	consumer.sid = consumer.sid or cm.subscribe(consumer.stream.client, cm.new_inbox(), function(m) return m end)
	setmetatable(consumer, consumer_metatable)
	consumer.stream.consumers[consumer.name] = consumer
	return consumer
end

local function consumer_create(stream, data) return consumer_update({}, data, stream) end

-- stream:consumer(config|name [, opts] [, action])  action = 'info' | 'create' | 'update' | nil
function js.consumer(stream, config, opts, action)
	if action == nil and type(opts) == 'string' then
		action = opts
		opts = nil
	end
	if type(config) == 'string' then
		config = { name = config }
	end
	config = type(config) == 'table' and config or {}
	opts = type(opts) == 'table' and opts or {}

	-- legacy alias: durable = true | '<name>'
	if config.durable ~= nil and not config.durable_name then
		if type(config.durable) == 'string' and valid_consumer_name(config.durable) then
			config.durable_name = config.durable
		elseif config.durable == true and valid_consumer_name(config.name) then
			config.durable_name = config.name
		elseif config.durable then
			return nil, 'invalid durable name'
		end
	end
	config.durable = nil
	if config.durable_name and not valid_consumer_name(config.durable_name) then return nil, 'invalid durable name' end
	if config.name and not valid_consumer_name(config.name) then return nil, 'invalid consumer name' end
	config.name = config.durable_name or config.name or consumer_nuid()

	local client = stream.client
	local consumer = stream.consumers[config.name]
	local msg, err
	if not consumer or action == 'info' then
		msg, err = api(client, stream.api .. 'CONSUMER.INFO.' .. stream.name .. '.' .. config.name)
		if msg then
			consumer = consumer and consumer_update(consumer, msg.data, stream) or consumer_create(stream, msg.data)
		elseif type(err) ~= 'table' then
			return nil, err
		elseif action == 'info' or action == 'update' then
			return nil, 'consumer not found', err
		end
		if action == 'info' then return consumer, msg end
	end

	local req = { config = config, stream_name = stream.name, action = action }
	local subj = stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. config.name
	if config.filter_subject and valid_subject(config.filter_subject) then
		subj = subj .. '.' .. config.filter_subject
	end
	msg, err = api(client, subj, encode(req))
	if not msg then return nil, err end
	consumer = consumer and consumer_update(consumer, msg.data, stream) or consumer_create(stream, msg.data)
	return consumer, msg
end

function js.get_consumer(stream, name) return js.consumer(stream, name, 'info') end
function js.create_consumer(stream, config) return js.consumer(stream, config, 'create') end
function js.update_consumer(stream, config) return js.consumer(stream, config, 'update') end
function js.create_update_consumer(stream, config) return js.consumer(stream, config) end
function js.delete_consumer(stream, consumer_name)
	local c = stream.consumers[consumer_name]
	if c then return con.delete(c) end
	local msg, err = api(stream.client, stream.api .. 'CONSUMER.DELETE.' .. stream.name .. '.' .. consumer_name)
	if not msg then return false, err end
	return true, msg
end
-- TODO paging support for information and not objects
function js.consumer_list(stream)
	local l, err = paged(stream.client, stream.api .. 'CONSUMER.LIST.' .. stream.name, 'consumers')
	if not l then return false, err end
	return { consumers = l, total = #l }
end
function js.consumer_names(stream)
	return paged(stream.client, stream.api .. 'CONSUMER.NAMES.' .. stream.name, 'consumers')
end

-- ADR-42 / consumer pause: pause_consumer(name, until_t) until_t = unix seconds or RFC 3339 string
local function pause_req(stream, name, until_t)
	local req = {}
	if until_t then req.pause_until = type(until_t) == 'number' and nats.timestamp(floor(until_t)) or until_t end
	local m, err = api(stream.client, stream.api .. 'CONSUMER.PAUSE.' .. stream.name .. '.' .. name, encode(req))
	if not m then return nil, err end
	return m.data
end
function js.pause_consumer(stream, name, until_t) return pause_req(stream, name, until_t or os.time() + 3600) end
function js.resume_consumer(stream, name) return pause_req(stream, name, nil) end
function js.unpin_consumer(stream, name, group)
	local m, err = api(stream.client, stream.api .. 'CONSUMER.UNPIN.' .. stream.name .. '.' .. name,
		encode({ group = group }))
	if not m then return nil, err end
	return true, m
end

function con.delete(consumer)
	local stream = consumer.stream
	local msg, err = api(stream.client, stream.api .. 'CONSUMER.DELETE.' .. stream.name .. '.' .. consumer.name)
	if not msg then return false, err end
	cm.unsubscribe(stream.client, consumer.sid)
	stream.consumers[consumer.name] = nil
	return true, msg
end

function con.pause(consumer, until_t) return js.pause_consumer(consumer.stream, consumer.name, until_t) end
function con.resume(consumer) return js.resume_consumer(consumer.stream, consumer.name) end
function con.unpin(consumer, group) return js.unpin_consumer(consumer.stream, consumer.name, group) end

function con.info(consumer)
	return js.consumer(consumer.stream, consumer.name, 'info')
end

function con.update(consumer, config, opts)
	config = config or {}
	config.name = config.name or consumer.name
	return js.consumer(consumer.stream, config, opts, 'update')
end

-- fetch(n, max_bytes, timeout_seconds, heartbeat_seconds) -> array of msgs (ADR-13)
-- timeout <= 0 (or nil) uses no_wait. Status messages (404/408/409) end the batch,
-- idle heartbeats (100) are skipped. Payloads are returned as received.
local function priority_fields(consumer, req, opts)
	if not opts then return req end
	if opts.group then
		req.group = opts.group
		req.min_pending = opts.min_pending
		req.min_ack_pending = opts.min_ack_pending
		req.priority = opts.priority
		req.id = opts.id or consumer.pin_id
	end
	return req
end
nats.jetstream.priority_fields = priority_fields

-- fetch(n, max_bytes, timeout, heartbeat [, opts]) opts: group, min_pending, min_ack_pending, priority, id
function con.fetch(consumer, n, b, to, hb, popts)
	n = tonumber(n) or 1
	local stream = consumer.stream
	local client = unwrap(stream.client)
	local req = priority_fields(consumer, { batch = n, max_bytes = tonumber(b) }, popts)
	local wait_t
	to = tonumber(to)
	if to and to > 0 then
		req.expires = floor(to * NS)
		wait_t = to + 1
	else
		req.no_wait = true
		wait_t = client.config.timeout
	end
	if tonumber(hb) then req.idle_heartbeat = floor(tonumber(hb) * NS) end
	local s = client.subscriptions[consumer.sid]
	if not s then return {}, 'consumer inbox not subscribed' end
	cm.publish(client, stream.api .. 'CONSUMER.MSG.NEXT.' .. stream.name .. '.' .. consumer.name, encode(req), s.subject)
	local msgs, deadline = {}, gettime() + wait_t
	while #msgs < n do
		local rem = deadline - gettime()
		if rem <= 0 then break end
		local m = cm.wait(client, { sid = consumer.sid, timeout = rem })
		if not m then break end
		local st = rawget(m, 'status_code')
		if not st then
			local pin = popts and popts.group and m:headers()['Nats-Pin-Id']
			if pin then consumer.pin_id = pin end
			msgs[#msgs + 1] = m
		elseif st ~= '100' then
			if st == '423' then consumer.pin_id = nil end
			if #msgs == 0 then return msgs, rawget(m, 'status_description') or st end
			break
		end
	end
	return msgs
end

function con.next(consumer, to, hb)
	local msgs, err = con.fetch(consumer, 1, nil, to, hb)
	return msgs[1], err
end

-- ---------------------------------------------------------------------------
-- Continuous pull consumption (ADR-37)
-- consumer:consume(callback(msg) [, opts]) -> ConsumeContext
-- opts: max_messages (500), max_bytes, expires (30 s), idle_heartbeat (min(expires/2, 30) s),
--       threshold_messages (50%), threshold_bytes (50%), error_cb(ctx, err, status),
--       group, min_pending, min_ack_pending, priority (ADR-42 priority groups)
-- Messages are delivered while the application pumps the connection (client:wait()).
-- ---------------------------------------------------------------------------
local ConsumeCtx = {}
ConsumeCtx.__index = ConsumeCtx
nats.jetstream.consume_context = ConsumeCtx

local function msg_bytes(m)
	local h = m.header
	return #(m.subject or '') + #(m.reply or '') + #(m.data or '') + (type(h) == 'string' and #h or 0)
end

local function ctx_error(ctx, err, st)
	ctx.last_error = err
	local cb = ctx.opts.error_cb
	if cb then pcall(cb, ctx, err, st) end
end

function ConsumeCtx:pull()
	if not self.running then return end
	local o = self.opts
	local batch = self.max_msgs - self.pending_msgs
	local mb
	if self.max_bytes then
		mb = self.max_bytes - self.pending_bytes
		if mb <= 0 then return end
	end
	if batch <= 0 then return end
	local req = priority_fields(self.consumer, {
		batch = batch, max_bytes = mb,
		expires = floor(self.expires * NS),
		idle_heartbeat = floor(self.hb * NS),
	}, o.group and o or nil)
	local stream = self.consumer.stream
	cm.publish(self.client, stream.api .. 'CONSUMER.MSG.NEXT.' .. stream.name .. '.' .. self.consumer.name,
		encode(req), self.inbox)
	self.pending_msgs = self.pending_msgs + batch
	if mb then self.pending_bytes = self.pending_bytes + mb end
	self.pulls = self.pulls + 1
end

function ConsumeCtx:reset_pending()
	self.pending_msgs, self.pending_bytes = 0, 0
end

local function below_threshold(ctx)
	if ctx.pending_msgs <= ctx.thr_msgs then return true end
	if ctx.max_bytes and ctx.pending_bytes <= ctx.thr_bytes then return true end
	return false
end

function ConsumeCtx:on_msg(m)
	self.last_activity = gettime()
	local st = rawget(m, 'status_code')
	if not st then
		self.pending_msgs = self.pending_msgs - 1
		if self.max_bytes then self.pending_bytes = self.pending_bytes - msg_bytes(m) end
		self.delivered = self.delivered + 1
		local o = self.opts
		if o.group then
			local pin = m:headers()['Nats-Pin-Id']
			if pin and pin ~= self.consumer.pin_id then
				self.consumer.pin_id = pin
				if o.pinned_cb then pcall(o.pinned_cb, self, pin) end
			end
		end
		local ok, err = pcall(self.callback, m)
		if not ok then ctx_error(self, 'consume callback error: ' .. tostring(err)) end
		if self.running and below_threshold(self) then self:pull() end
		return
	end
	if st == '100' then -- idle heartbeat
		return
	end
	local desc = rawget(m, 'status_description') or ''
	local h = m:headers()
	local pm, pb = tonumber(h['Nats-Pending-Messages']), tonumber(h['Nats-Pending-Bytes'])
	if pm then
		self.pending_msgs = max(0, self.pending_msgs - pm)
		if self.max_bytes then self.pending_bytes = max(0, self.pending_bytes - (pb or 0)) end
	end
	if st == '408' or st == '404' then
		if not pm then self:reset_pending() end
	elseif st == '409' then
		local d = desc:lower()
		if find(d, 'consumer deleted', 1, true) or find(d, 'consumer is push based', 1, true) then
			ctx_error(self, desc, st)
			if self.terminal_cb then self.terminal_cb(self, desc) else self:stop() end
			return
		end
		if not pm then self:reset_pending() end
		ctx_error(self, desc, st)
	elseif st == '423' then -- pin id mismatch: drop our pin and re-pull as a candidate
		self.consumer.pin_id = nil
		self:reset_pending()
		ctx_error(self, desc, st)
	else
		ctx_error(self, desc ~= '' and desc or st, st)
	end
	if self.running and below_threshold(self) then self:pull() end
end

function ConsumeCtx:check(now)
	if not self.running then return false end
	if not self.client.connected then return end
	if now - self.last_activity > 2 * self.hb then -- ADR-37: missed heartbeats -> reset and re-pull
		self.missed = self.missed + 1
		self.last_activity = now
		ctx_error(self, 'missed idle heartbeat', '100')
		if self.missed_cb then self.missed_cb(self) return end
		self:reset_pending()
		self:pull()
	elseif self.pending_msgs <= 0 then
		self:pull()
	end
end

function ConsumeCtx:stop()
	if not self.running and not self.sid then return end
	self.running = false
	if self.sid then
		pcall(cm.unsubscribe, self.client, self.sid)
		self.sid = nil
	end
	if self.ticker then cm.remove_ticker(self.client, self.ticker) self.ticker = nil end
	if self.rc_hook then cm.off_reconnect(self.client, self.rc_hook) self.rc_hook = nil end
	if self.on_stop then pcall(self.on_stop, self) end
end

-- drain: stop pulling, deliver what is already buffered/in flight (up to timeout), then stop
function ConsumeCtx:drain(timeout)
	self.running = false
	local deadline = gettime() + (timeout or 5)
	while self.pending_msgs > 0 and gettime() < deadline do
		if not cm.wait(self.client, { sid = self.sid, timeout = min(0.25, deadline - gettime()) }) then break end
	end
	self.running = true
	self:stop()
end

function ConsumeCtx:closed() return not self.running end

local function consume(consumer, callback, opts)
	if type(callback) ~= 'function' then return nil, 'callback required' end
	opts = type(opts) == 'table' and opts or {}
	local client = unwrap(consumer.stream.client)
	local max_bytes = tonumber(opts.max_bytes)
	local max_msgs = tonumber(opts.max_messages) or (max_bytes and 1000000) or 500
	local expires = tonumber(opts.expires) or 30
	if expires < 1 then expires = 1 end
	local hb = tonumber(opts.idle_heartbeat) or min(expires / 2, 30)
	local ctx = setmetatable({
		consumer = consumer, client = client, callback = callback, opts = opts,
		max_msgs = max_msgs, max_bytes = max_bytes, expires = expires, hb = hb,
		thr_msgs = tonumber(opts.threshold_messages) or floor(max_msgs / 2),
		thr_bytes = tonumber(opts.threshold_bytes) or (max_bytes and floor(max_bytes / 2)),
		pending_msgs = 0, pending_bytes = 0, delivered = 0, pulls = 0, missed = 0,
		last_activity = gettime(), running = true,
		inbox = cm.new_inbox(),
	}, ConsumeCtx)
	ctx.sid = cm.subscribe(client, ctx.inbox, function(m) ctx:on_msg(m) return nil end)
	ctx.ticker = cm.add_ticker(client, min(hb, 1), function(_, now) return ctx:check(now) end)
	ctx.rc_hook = cm.on_reconnect(client, function() -- pull requests do not survive a reconnect
		ctx:reset_pending()
		ctx.last_activity = gettime()
		ctx:pull()
	end)
	ctx:pull()
	return ctx
end
con.consume = consume

-- messages([opts]) -> iterator object: it:next([timeout]) -> msg | nil, 'timeout'; it:stop()
local MsgIter = {}
MsgIter.__index = MsgIter
function MsgIter:next(timeout)
	local q = self.queue
	if #q == 0 then
		local deadline = gettime() + (timeout or 30)
		while #q == 0 do
			local left = deadline - gettime()
			if left <= 0 or not self.ctx.running then break end
			cm.wait(self.ctx.client, { timeout = left, maxcount = 1 })
		end
	end
	if #q == 0 then return nil, self.ctx.running and 'timeout' or 'stopped' end
	return table.remove(q, 1)
end
function MsgIter:stop() self.ctx:stop() end
MsgIter.drain = MsgIter.stop
function con.messages(consumer, opts)
	opts = type(opts) == 'table' and opts or {}
	local it = setmetatable({ queue = {} }, MsgIter)
	local ctx, err = consume(consumer, function(m) it.queue[#it.queue + 1] = m end, opts)
	if not ctx then return nil, err end
	it.ctx = ctx
	return it
end
function con.stop() return nil, 'use the context returned by consume():stop()' end
function con.drain() return nil, 'use the context returned by consume():drain()' end

-- ---------------------------------------------------------------------------
-- Ordered consumer (ADR-17, ADR-37): ephemeral, ack none, single replica, memory
-- storage. Tracks stream/consumer sequences and transparently recreates the
-- consumer from the next stream sequence after gaps, missed heartbeats or the
-- consumer disappearing.
-- stream:ordered_consumer([cfg]) cfg: filter_subject(s), deliver_policy, opt_start_seq,
--   opt_start_time, replay_policy, headers_only, inactive_threshold (s, default 300),
--   max_reset_attempts (-1 = unlimited), name_prefix
-- ---------------------------------------------------------------------------
local Ordered = {}
Ordered.__index = Ordered
nats.jetstream.ordered_metatable = Ordered

function js.ordered_consumer(stream, cfg)
	cfg = type(cfg) == 'table' and cfg or {}
	return setmetatable({ stream = stream, cfg = cfg, stream_seq = 0, consumer_seq = 0, serial = 0,
		resets = 0, prefix = cfg.name_prefix or consumer_nuid() }, Ordered)
end

function Ordered:config()
	local c = self.cfg
	local cc = {
		name = self.prefix .. '_' .. self.serial,
		ack_policy = 'none', max_deliver = 1, mem_storage = true, num_replicas = 1,
		inactive_threshold = floor((c.inactive_threshold or 300) * NS),
		deliver_policy = c.deliver_policy or 'all',
		opt_start_seq = c.opt_start_seq, opt_start_time = c.opt_start_time,
		replay_policy = c.replay_policy or 'instant', headers_only = c.headers_only or nil,
		metadata = c.metadata,
	}
	local fs = c.filter_subjects
	if type(fs) == 'table' and #fs == 1 then cc.filter_subject = fs[1]
	elseif type(fs) == 'table' and #fs > 1 then cc.filter_subjects = fs
	elseif c.filter_subject then cc.filter_subject = c.filter_subject end
	if self.stream_seq > 0 then
		cc.deliver_policy = 'by_start_sequence'
		cc.opt_start_seq = self.stream_seq + 1
		cc.opt_start_time = nil
	end
	return cc
end

function Ordered:create()
	self.serial = self.serial + 1
	self.consumer_seq = 0
	local c, err = js.consumer(self.stream, self:config(), 'create')
	if not c then return nil, err end
	self.consumer = c
	return c
end

function Ordered:delete_current()
	local c = self.consumer
	self.consumer = nil
	if c then
		c.stream.consumers[c.name] = nil
		pcall(cm.unsubscribe, c.stream.client, c.sid)
		-- best effort, do not wait for the reply
		pcall(cm.request, c.stream.client, c.stream.api .. 'CONSUMER.DELETE.' .. c.stream.name .. '.' .. c.name)
	end
end

-- returns true if m is in order (and advances), false if a reset is required
function Ordered:track(m)
	local meta = mm.metafields(m)
	if self.consumer and meta.consumer and meta.consumer ~= self.consumer.name then return nil end -- stale
	local cseq = meta.consumer_seq
	if cseq and cseq ~= self.consumer_seq + 1 then return false end
	self.consumer_seq = cseq or self.consumer_seq + 1
	self.stream_seq = meta.stream_seq or self.stream_seq
	return true, meta
end

function Ordered:reset(reason)
	self.resets = self.resets + 1
	local maxr = self.cfg.max_reset_attempts
	if maxr and maxr >= 0 and self.resets > maxr then return nil, 'ordered consumer: too many resets' end
	self:delete_current()
	local c, err = self:create()
	if not c then return nil, err end
	if self.cfg.reset_cb then pcall(self.cfg.reset_cb, self, reason) end
	return c
end

function Ordered:info() if self.consumer then return con.info(self.consumer) end end

-- fetch(n [, timeout]) -> msgs (in order); recreates the consumer when needed
function Ordered:fetch(n, to)
	if not self.consumer then
		local c, err = self:create()
		if not c then return {}, err end
	end
	local msgs, err = con.fetch(self.consumer, n, nil, to or 5)
	local out = {}
	for _, m in ipairs(msgs) do
		local ok = self:track(m)
		if ok == false then
			self:reset('sequence gap')
			break
		elseif ok then out[#out + 1] = m end
	end
	if #out == 0 and err and find(tostring(err), 'not found', 1, true) then self:reset(err) end
	return out, (#out == 0) and err or nil
end
function Ordered:next(to) local m, err = self:fetch(1, to) return m[1], err end

-- consume(callback(msg) [, opts]) -> context with :stop()
function Ordered:consume(callback, opts)
	if type(callback) ~= 'function' then return nil, 'callback required' end
	opts = type(opts) == 'table' and opts or {}
	if not self.consumer then
		local c, err = self:create()
		if not c then return nil, err end
	end
	local me = self
	local outer = { running = true, ordered = me }
	local function start()
		local ctx
		local function restart(reason)
			if not outer.running then return end
			ctx.running = false
			ctx:stop()
			local c, err = me:reset(reason)
			if not c then
				outer.running = false
				outer.last_error = err
				if opts.error_cb then pcall(opts.error_cb, outer, err) end
				return
			end
			start()
		end
		ctx = consume(me.consumer, function(m)
			local ok = me:track(m)
			if ok == false then return restart('sequence gap') end
			if ok then return callback(m) end
		end, opts)
		ctx.terminal_cb = function(_, why) restart(why) end
		ctx.missed_cb = function() restart('missed heartbeat') end
		outer.ctx = ctx
	end
	start()
	function outer.stop() outer.running = false if outer.ctx then outer.ctx:stop() end me:delete_current() end
	function outer.closed() return not outer.running end
	outer.client = unwrap(self.stream.client)
	return setmetatable(outer, { __index = function(t, k) return t.ctx and t.ctx[k] end })
end

function Ordered:messages(opts)
	local it = setmetatable({ queue = {} }, MsgIter)
	local ctx, err = self:consume(function(m) it.queue[#it.queue + 1] = m end, opts)
	if not ctx then return nil, err end
	it.ctx = ctx
	return it
end
function Ordered:stop() self:delete_current() end

-- ---------------------------------------------------------------------------
-- Push consumers with flow control and idle heartbeats (ADR-9), optionally ordered (ADR-17)
-- stream:subscribe(cfg, callback(msg) [, opts]) -> PushSub {stop()}
-- cfg is a consumer config (deliver_subject defaults to a new inbox). opts:
--   idle_heartbeat (seconds, default 5 when flow_control/ordered), flow_control (bool),
--   ordered (bool), queue (deliver_group), error_cb(sub, err), manual_ack
-- ---------------------------------------------------------------------------
local PushSub = {}
PushSub.__index = PushSub

function js.subscribe(stream, cfg, callback, opts)
	if type(cfg) == 'function' then cfg, callback, opts = {}, cfg, callback end
	if type(callback) ~= 'function' then return nil, 'callback required' end
	cfg = type(cfg) == 'table' and cfg or {}
	opts = type(opts) == 'table' and opts or {}
	local client = unwrap(stream.client)
	local sub = setmetatable({ stream = stream, client = client, opts = opts, running = true,
		last_activity = gettime(), missed = 0, fc_replies = 0, stalls = 0 }, PushSub)
	local hb = tonumber(opts.idle_heartbeat) or ((opts.ordered or opts.flow_control or cfg.flow_control) and 5)
	sub.hb = hb
	local ccfg = {}
	for k, v in pairs(cfg) do ccfg[k] = v end
	ccfg.deliver_subject = ccfg.deliver_subject or cm.new_inbox()
	if opts.queue then ccfg.deliver_group = opts.queue end
	if hb then ccfg.idle_heartbeat = floor(hb * NS) end
	if opts.flow_control or opts.ordered then ccfg.flow_control = true end
	sub.deliver = ccfg.deliver_subject
	if opts.ordered then
		local oc = js.ordered_consumer(stream, cfg)
		sub.ordered = oc
		local base = oc.config
		oc.config = function(self)
			local c = base(self)
			c.deliver_subject = sub.deliver
			c.flow_control = true
			c.idle_heartbeat = floor((hb or 5) * NS)
			return c
		end
	end
	local function on_msg(m)
		sub.last_activity = gettime()
		local st = rawget(m, 'status_code')
		if st == '100' then
			local desc = rawget(m, 'status_description') or ''
			if find(desc, 'FlowControl', 1, true) then
				if m.reply then cm.publish(client, m.reply, '') sub.fc_replies = sub.fc_replies + 1 end
			else -- idle heartbeat
				local h = m:headers()
				local stalled = h['Nats-Consumer-Stalled']
				if stalled then cm.publish(client, stalled, '') sub.stalls = sub.stalls + 1 end
				local oc = sub.ordered
				local lc = tonumber(h['Nats-Last-Consumer'])
				if oc and lc and lc > oc.consumer_seq then sub:reset('gap detected by heartbeat') end
			end
			return
		elseif st then
			if opts.error_cb then pcall(opts.error_cb, sub, rawget(m, 'status_description') or st) end
			return
		end
		local oc = sub.ordered
		if oc then
			local ok = oc:track(m)
			if ok == false then return sub:reset('sequence gap') end
			if ok == nil then return end
		end
		local ok, err = pcall(callback, m)
		if not ok and opts.error_cb then pcall(opts.error_cb, sub, err) end
	end
	sub.sid = cm.subscribe(client, sub.deliver, function(m) on_msg(m) return nil end, opts.queue)
	local c, err
	if sub.ordered then c, err = sub.ordered:create()
	else c, err = js.consumer(stream, ccfg, opts.action) end
	if not c then cm.unsubscribe(client, sub.sid) return nil, err end
	sub.consumer = c
	if hb then
		sub.ticker = cm.add_ticker(client, hb, function(_, now)
			if not sub.running then return false end
			if not client.connected then return end
			if now - sub.last_activity > 2 * hb then -- ADR-9: two missed heartbeats
				sub.missed = sub.missed + 1
				sub.last_activity = now
				if opts.error_cb then pcall(opts.error_cb, sub, 'missed idle heartbeat') end
				if sub.ordered then sub:reset('missed heartbeat') end
			end
		end)
	end
	return sub
end

function PushSub:reset(reason)
	local oc = self.ordered
	if not oc then return end
	local c, err = oc:reset(reason)
	if c then self.consumer = c
	elseif self.opts.error_cb then pcall(self.opts.error_cb, self, err) end
end

function PushSub:stop()
	self.running = false
	if self.sid then pcall(cm.unsubscribe, self.client, self.sid) self.sid = nil end
	if self.ticker then cm.remove_ticker(self.client, self.ticker) self.ticker = nil end
	if self.ordered then self.ordered:delete_current()
	elseif self.consumer and not self.consumer.durable and not self.opts.keep then
		pcall(con.delete, self.consumer)
	end
end
PushSub.unsubscribe = PushSub.stop

-- ---------------------------------------------------------------------------
-- Direct get batch / multi last (ADR-31)
-- stream:direct_get(req) req: seq, next_by_subj, last_by_subj, batch, max_bytes,
--   multi_last = {subjects}, up_to_seq, up_to_time, start_time
-- returns array of msgs, eob {num_pending=, last_seq=, up_to_seq=} or nil, err
-- ---------------------------------------------------------------------------
function js.direct_get(stream, req, timeout)
	req = req or {}
	local client = unwrap(stream.client)
	local eob, err = {}, nil
	local msgs, rerr = cm.request_many(client, stream.api .. 'DIRECT.GET.' .. stream.name, encode(req), {
		max_wait = timeout or client.config.timeout or 5,
		max_messages = (not req.batch and not req.multi_last) and 1 or nil,
		sentinel = function(m)
			local st = rawget(m, 'status_code')
			if not st then return false end
			local h = m:headers()
			eob.num_pending = tonumber(h['Nats-Num-Pending'])
			eob.last_seq = tonumber(h['Nats-Last-Sequence'])
			eob.up_to_seq = tonumber(h['Nats-UpTo-Sequence'])
			if st ~= '204' then err = st == '404' and 'message not found' or (rawget(m, 'status_description') or st) end
			return true
		end,
	})
	if rerr then return nil, rerr end
	if err and #msgs == 0 then return nil, err end
	return msgs, eob
end

-- ---------------------------------------------------------------------------
-- Counters (ADR-49): streams with allow_msg_counter = true
-- client:js_counter_add(subject, delta) -> value (number), ack | nil, err
-- stream:counter_get(subject) -> value, msg
-- ---------------------------------------------------------------------------
function cm.js_counter_add(client, subject, delta, opts)
	local o = {}
	for k, v in pairs(opts or {}) do o[k] = v end
	o.incr = delta or 1
	local ack, err = js_publish(client, subject, nil, o)
	if not ack then return nil, err end
	return tonumber(ack.val), ack
end
function js.counter_add(stream, subject, delta) return cm.js_counter_add(stream.client, subject, delta) end
function js.counter_get(stream, subject)
	local m, err = js.getmsg(stream, subject)
	if not m then return nil, err end
	local ok, d = pcall(decode, m.data)
	if not ok or type(d) ~= 'table' then return nil, 'invalid counter value' end
	return tonumber(d.val), m
end

-- ---------------------------------------------------------------------------
-- Atomic batch publish (ADR-50): stream needs allow_atomic = true
-- client:js_publish_batch({ {subject=, data=, header=}, ... } [, opts]) -> pub ack | nil, err
-- ---------------------------------------------------------------------------
function cm.js_publish_batch(client, msgs, opts)
	opts = type(opts) == 'table' and opts or {}
	local nc = unwrap(client)
	local n = #msgs
	if n == 0 then return nil, 'empty batch' end
	local id = opts.id or nats.nuid.next()
	for i, m in ipairs(msgs) do
		local h = m.header and decode_header(m.header) or {}
		if type(h) ~= 'table' then h = {} end
		h['Nats-Batch-Id'] = id
		h['Nats-Batch-Sequence'] = tostring(i)
		if i == n then h['Nats-Batch-Commit'] = '1' end
		if m.ttl then h['Nats-TTL'] = nats.jetstream.ttl_string(m.ttl) end
		if m.msg_id then h['Nats-Msg-Id'] = m.msg_id end
		local hdr = encode_header(h)
		if i == 1 or i == n or (opts.ack_every and i % opts.ack_every == 0) then
			local r, err = cm.get(nc, m.subject, m.data, hdr, nil, opts.timeout)
			if not r then return nil, err end
			if r.data and r.data ~= '' then
				local ok, ack = pcall(decode, r.data)
				if ok and type(ack) == 'table' then
					if ack.error then return nil, ack.error.description or 'batch publish error', ack end
					if i == n then return ack, r end
				end
			elseif i == n then return nil, 'empty reply to batch commit' end
		else
			cm.publish(nc, m.subject, m.data, nil, hdr)
		end
	end
end


-- Message Support
local function ack_msg(m, t, opt, ack) -- msg, type, opt(number delay seconds, string term reason)
	if rawget(m, 'ackd') then return false, "Message already acknowledged."
	elseif not m.reply then return false, "ErrMsgNoReply" end

	if t then
		if t == "-NAK" and type(opt) == 'number' then t = concat { "-NAK {\"delay\": ", format('%.0f', opt * NS), "}" }
		elseif t == "+TERM" and type(opt) == 'string' then t = concat { t, " ", opt } end
		if t ~= "+WPI" then rawset(m, 'ackd', true) end
	end

	local client = unwrap(m.client)
	if ack or t == "+NXT" then return cm.get(client, m.reply, t) end
	return cm.publish(client, m.reply, t)
end

function mm.acked(m) return rawget(m, 'ackd') end
function mm.ack(m, t, opt, ack) return ack_msg(m, t or "+ACK", opt, ack) end
function mm.ack_ack(m) return ack_msg(m, "+ACK", nil, true) end
mm.double_ack = mm.ack_ack
function mm.nak(m, d) return ack_msg(m, "-NAK", d) end
function mm.term(m, s) return ack_msg(m, "+TERM", s) end
function mm.in_progress(m) return ack_msg(m, "+WPI") end
mm.wpi = mm.in_progress
function mm.ack_next(m) return ack_msg(m, "+NXT") end

function mm.pending(m)
	local h = mm.headers(m)
	return tonumber(h["Nats-Pending-Messages"]), tonumber(h["Nats-Pending-Bytes"])
end

local str_explode = function(s, ss)
	local t, i = {}, 1
	for st in gmatch(s, "([^" .. ss .. "]+)") do
		t[i] = st
		i = i + 1
	end
	return t, i - 1
end

-- JetStream ack subject metadata: $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
function mm.metafields(m)
	if not m.reply then return {} end
	local s, tc = str_explode(m.reply, ".")
	if s[1] == "$JS" and s[2] == "ACK" then
		if tc >= 12 then
			return {
				domain = s[3] ~= '_' and s[3] or nil,
				account_hash = s[4],
				stream = s[5],
				consumer = s[6],
				delivered = tonumber(s[7]),
				stream_seq = tonumber(s[8]),
				consumer_seq = tonumber(s[9]),
				timestamp = tonumber(s[10]),
				pending = tonumber(s[11]),
				token = s[12],
			}
		elseif tc == 9 then
			return {
				stream = s[3],
				consumer = s[4],
				delivered = tonumber(s[5]),
				stream_seq = tonumber(s[6]),
				consumer_seq = tonumber(s[7]),
				timestamp = tonumber(s[8]),
				pending = tonumber(s[9]),
			}
		end
	end
	return {}
end

return nats

-- type ConsumerConfig struct {
	-- // Durable is deprecated. All consumers should have names, picked by clients.
	-- Durable         string          `json:"durable_name,omitempty"`
	-- Name            string          `json:"name,omitempty"`
	-- Description     string          `json:"description,omitempty"`
	-- DeliverPolicy   DeliverPolicy   `json:"deliver_policy"`
	-- OptStartSeq     uint64          `json:"opt_start_seq,omitempty"`
	-- OptStartTime    *time.Time      `json:"opt_start_time,omitempty"`
	-- ack_policy       ack_policy       `json:"ack_policy"`
	-- ack_wait         time.Duration   `json:"ack_wait,omitempty"`
	-- max_deliver      int             `json:"max_deliver,omitempty"`
	-- BackOff         []time.Duration `json:"backoff,omitempty"`
	-- FilterSubject   string          `json:"filter_subject,omitempty"`
	-- FilterSubjects  []string        `json:"filter_subjects,omitempty"`
	-- ReplayPolicy    ReplayPolicy    `json:"replay_policy"`
	-- RateLimit       uint64          `json:"rate_limit_bps,omitempty"` // Bits per sec
	-- SampleFrequency string          `json:"sample_freq,omitempty"`
	-- MaxWaiting      int             `json:"max_waiting,omitempty"`
	-- MaxAckPending   int             `json:"max_ack_pending,omitempty"`
	-- Heartbeat       time.Duration   `json:"idle_heartbeat,omitempty"`
	-- FlowControl     bool            `json:"flow_control,omitempty"`
	-- HeadersOnly     bool            `json:"headers_only,omitempty"`

	-- // Pull based options.
	-- MaxRequestBatch    int           `json:"max_batch,omitempty"`
	-- MaxRequestExpires  time.Duration `json:"max_expires,omitempty"`
	-- MaxRequestMaxBytes int           `json:"max_bytes,omitempty"`

	-- // Push based consumers.
	-- deliver_subject string `json:"deliver_subject,omitempty"`
	-- DeliverGroup   string `json:"deliver_group,omitempty"`

	-- // Ephemeral inactivity threshold.
	-- InactiveThreshold time.Duration `json:"inactive_threshold,omitempty"`

	-- // Generally inherited by parent stream and other markers, now can be configured directly.
	-- num_replicas int `json:"num_replicas"`
	-- // Force memory storage.
	-- MemoryStorage bool `json:"mem_storage,omitempty"`

	-- // Don't add to general clients.
	-- Direct bool `json:"direct,omitempty"`

	-- // Metadata is additional metadata for the Consumer.
	-- Metadata map[string]string `json:"metadata,omitempty"`

	-- // PauseUntil is for suspending the consumer until_t the deadline.
	-- PauseUntil *time.Time `json:"pause_until,omitempty"`
-- }

-- pullRequest struct {
	-- Expires   time.Duration `json:"expires,omitempty"`
	-- Batch     int           `json:"batch,omitempty"`
	-- MaxBytes  int           `json:"max_bytes,omitempty"`
	-- NoWait    bool          `json:"no_wait,omitempty"`
	-- Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
-- }

-- // Headers used when publishing messages.
-- const (
	-- // MsgIdHeader is used to specify a user-defined message ID. It can be used
	-- // e.g. for deduplication in conjunction with the Duplicates duration on
	-- // ConsumerConfig or to provide optimistic concurrency safety together with
	-- // [ExpectedLastMsgIDHeader].
	-- //
	-- // This can be set when publishing messages using [WithMsgID] option.
	-- MsgIDHeader = "Nats-Msg-Id"

	-- // ExpectedStreamHeader contains stream name and is used to assure that the
	-- // published message is received by expected stream. Server will reject the
	-- // message if it is not the case.
	-- //
	-- // This can be set when publishing messages using [WithExpectStream] option.
	-- ExpectedStreamHeader = "Nats-Expected-Stream"

	-- // ExpectedLastSeqHeader contains the expected last sequence number of the
	-- // stream and can be used to apply optimistic concurrency control at stream
	-- // level. Server will reject the message if it is not the case.
	-- //
	-- // This can be set when publishing messages using [WithExpectLastSequence]
	-- // option. option.
	-- ExpectedLastSeqHeader = "Nats-Expected-Last-Sequence"

	-- // ExpectedLastSubjSeqHeader contains the expected last sequence number on
	-- // the subject and can be used to apply optimistic concurrency control at
	-- // subject level. Server will reject the message if it is not the case.
	-- //
	-- // This can be set when publishing messages using
	-- // [WithExpectLastSequencePerSubject] option.
	-- ExpectedLastSubjSeqHeader = "Nats-Expected-Last-Subject-Sequence"

	-- // ExpectedLastMsgIDHeader contains the expected last message ID on the
	-- // subject and can be used to apply optimistic concurrency control at
	-- // stream level. Server will reject the message if it is not the case.
	-- //
	-- // This can be set when publishing messages using [WithExpectLastMsgID]
	-- // option.
	-- ExpectedLastMsgIDHeader = "Nats-Expected-Last-Msg-Id"

	-- // MsgRollup is used to apply a purge of all prior messages in the stream
	-- // ("all") or at the subject ("sub") before this message.
	-- MsgRollup = "Nats-Rollup"
-- )

-- // Headers for republished messages and direct gets. Those headers are set by
-- // the server and should not be set by the client.
-- const (
	-- // StreamHeader contains the stream name the message was republished from or
	-- // the stream name the message was retrieved from using direct get.
	-- StreamHeader = "Nats-Stream"

	-- // SequenceHeader contains the original sequence number of the message.
	-- SequenceHeader = "Nats-Sequence"

	-- // TimeStampHeader contains the original timestamp of the message.
	-- TimeStampHeaer = "Nats-Time-Stamp"

	-- // SubjectHeader contains the original subject the message was published to.
	-- SubjectHeader = "Nats-Subject"

	-- // LastSequenceHeader contains the last sequence of the message having the
	-- // same subject, otherwise zero if this is the first message for the
	-- // subject.
	-- LastSequenceHeader = "Nats-Last-Sequence"
-- )

-- // Rollups, can be subject only or all messages.
-- const (
	-- // MsgRollupSubject is used to purge all messages before this message on the
	-- // message subject.
	-- MsgRollupSubject = "sub"

	-- // MsgRollupAll is used to purge all messages before this message on the
	-- // stream.
	-- MsgRollupAll = "all"
-- )

-- var (
	-- ackAck      ackType = []byte("+ACK")
	-- ackNak      ackType = []byte("-NAK")
	-- ackProgress ackType = []byte("+WPI")
	-- ackTerm     ackType = []byte("+TERM")
-- )



