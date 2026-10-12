-- nats-jetstream.lua - JetStream streams, consumers and acks for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- TODO JetStream Consumer Idle Heartbeats (ADR-9) for push consumers
-- TODO Ordered consumer / js.subscribe workflow (ADR-15, ADR-17)
-- TODO Account Information Functions

-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = nats.jetstream or {}

-- ### Local properties ###
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber, tostring, pcall = tonumber, tostring, pcall
local pairs, type = pairs, type
local floor = math.floor
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
function cm.stream_names(client, subject)
	local m, err = api(client, defaultAPIPrefix .. 'STREAM.NAMES', subject and encode({ subject = subject }) or nil)
	if not m then return nil, err end
	return type(m.data.streams) == 'table' and m.data.streams or {}
end
function cm.stream_by_subject(client, subject)
	local names, err = cm.stream_names(client, subject)
	if not names then return nil, err end
	return names[1]
end
-- TODO paging support for information and not objects
function cm.list_streams(client)
	local m, err = api(client, defaultAPIPrefix .. "STREAM.LIST")
	if not m then return false, err end
	return m.data
end
function cm.account_info(client)
	local m, err = api(client, defaultAPIPrefix .. "INFO")
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
local function js_publish(client, subject, data, opts)
	opts = type(opts) == 'table' and opts or {}
	local header = opts.header and decode_header(opts.header) or {}
	if type(header) ~= 'table' then header = {} end
	if opts.id then header['Nats-Msg-Id'] = opts.id end
	if opts.last_msg_id then header['Nats-Expected-Last-Msg-Id'] = opts.last_msg_id end
	if opts.stream then header['Nats-Expected-Stream'] = opts.stream end
	if opts.last_seq then header['Nats-Expected-Last-Sequence'] = tostring(opts.last_seq) end
	if opts.last_subject_seq then header['Nats-Expected-Last-Subject-Sequence'] = tostring(opts.last_subject_seq) end
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
	local msg, err = api(stream.client, stream.api .. 'CONSUMER.LIST.' .. stream.name)
	if not msg then return false, err end
	return msg.data
end

function con.delete(consumer)
	local stream = consumer.stream
	local msg, err = api(stream.client, stream.api .. 'CONSUMER.DELETE.' .. stream.name .. '.' .. consumer.name)
	if not msg then return false, err end
	cm.unsubscribe(stream.client, consumer.sid)
	stream.consumers[consumer.name] = nil
	return true, msg
end

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
function con.fetch(consumer, n, b, to, hb)
	n = tonumber(n) or 1
	local stream = consumer.stream
	local client = unwrap(stream.client)
	local req = { batch = n, max_bytes = tonumber(b) }
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
			msgs[#msgs + 1] = m
		elseif st ~= '100' then
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

function con.consume(consumer, n, opts) return nil, 'not implemented' end
function con.messages(consumer, n, opts) return nil, 'not implemented' end
function con.stop(consumer) return nil, 'not implemented' end
function con.drain(consumer) return nil, 'not implemented' end

-- push/ordered subscribe workflow (ADR-15/ADR-17) is not implemented yet
function js.subscribe() return nil, 'not implemented' end

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

	-- // PauseUntil is for suspending the consumer until the deadline.
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



