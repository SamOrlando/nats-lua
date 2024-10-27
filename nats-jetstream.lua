-- TODO Test and Prove Durable and Non Duable Streams
-- TODO JetStream Consumer Idle Heartbeats - https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-9.md
-- TODO Batch and Consumer Options - https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-13.md
-- TODO Account Information Functions

-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
-- TODO (for consumer name) local digest = require 'resty.digest'
--local sha256 = digest.new('sha256')

-- ### Local properties ### speed and neet laziness ftw
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local format, gmatch = string.format, string.gmatch
local insert, concat = table.insert, table.concat
local encode, decode = cjson.encode, cjson.decode
local encode_header, decode_header = nats.encode_header, nats.decode_header
local json_error_true = '{\"error\":true}'

-- copy go vars
local defaultAPIPrefix = "$JS.API."
local jsDomainT = "$JS.%s.API."
local jsExtDomainT = "$JS.%s.API"
local jsAPIPrefix = "%s."

-- quick local references and functions
local cm = nats.client_metatable.methods -- client meta methods
local mm = nats.msg_metatable.methods -- msg meta methods
local valid_subject = nats.valid_subject
function nats.valid_consumer_name(s)
    if type(s) ~= 'string' or #s == 0 or s:find("[>*. /\\]") then return nil end
    return s
end
local valid_consumer_name = nats.valid_consumer_name
nats.valid_stream_name = nats.valid_consumer_name
local valid_stream_name = nats.valid_stream_name

--
-- Streams
local stream_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex stream: " .. key .. " with a value of " .. tostring(value)) end,
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
	stream.config = data.config or stream.config
	stream.name = data.name or data.config.name or stream.name or stream.config.name
	stream.opts = {
			pre = opts.pre and format(jsAPIPrefix, opts.pre),
			maxpa = opts.maxpa or 4000, -- stupid var whats it for?
			wait = opts.wait,
			aecb = nil,
			domain = opts.domain,
			purge_opts = nil, -- table,
			stream_list_subject = nil, --?
			direct_get = nil,
			feature_flags = nil, -- table
		}
	stream.ts = data.ts or stream.ts -- or timestamp() -- TODO needed?
	setmetatable(stream, stream_metatable)
	stream.client.streams[stream.name] = stream
	return stream
end

local function stream_create(client, data, opts) return stream_update({}, data, opts, client) end

function cm.stream(client, scfg, opts, action)
	if not scfg then return nil, 'no stream name provided' end
	if not action then
		if type(opts) == 'string' then
			action = opts
			opts = nil
		end
		if type(scfg) == 'string' then
			scfg = {name=scfg}
			action = 'info'
		end
	end
	opts = type(opts) == 'table' and opts or {}
	local msg = {}
	
	if not valid_stream_name(scfg.name) then return nil, 'invalid stream name' end

	local client = client.client or client -- encoded connections and feels bad
	if not rawget(client, "streams") then rawset(client, "streams", {}) end -- insert stream info into client table -- TODO add hook for stream obj?
	
	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local stream = client.streams[scfg.name] -- lets find our stream
	if not stream or action == 'info' then -- if not, lets query server
		msg = client:get((stream and stream.api or stream_api) .. 'STREAM.INFO.' .. scfg.name) -- we only check stream.api here 
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then
			if action == 'info' or action == 'update' then return nil, 'stream not found' end
		end
		stream = stream and stream_update(stream, msg.data, opts, client) or stream_create(client, msg.data, opts) -- if no stream obj for this, lets make an object
	end
	
	if stream then
		if action == nil or action == 'update' then
			msg = client:get(stream.api .. 'STREAM.UPDATE.' .. stream.name, encode(scfg))
			msg.data = decode(msg.data or json_error_true)
			if msg.data.error then return nil, msg end
			stream = stream_update(stream, msg.data, opts, client)
		elseif action == 'info' then return stream, msg
		elseif action == 'create' then return nil, 'stream existed on create' end -- stream existed and we are creating
	elseif action == nil or action == 'create' then
		msg = client:get(stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return nil, msg end
		stream = stream_create(client, msg.data, opts)
	else
		return nil, 'stream not found'
	end
	return stream, msg
end

-- per StreamManager interface request
function cm.get_stream(name) return cm.stream(client, name, nil, 'info') end
function cm.create_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'create') end
function cm.update_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'update') end
function cm.create_update_stream(client, scfg, opts) return cm.stream(client, scfg, opts) end
function cm.stream_by_subject() end
function cm.delete_stream() end --remove for stream:delete()?
function cm.stream_names() end
-- TODO paging support for information and not objects
function cm.list_streams(client)
	local msg = client:get("$JS.API.STREAM.LIST")
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.update(stream, scfg, opts) -- update server and update obj
	return cm.stream(stream.client, scfg or self.name, opts, 'update')
end

function js.info(stream) -- get info from server
	return cm.stream(stream.client, self.name, 'info')
end

function js.delete(stream)
	-- make request
	local msg = stream.client:get(stream.api .. 'STREAM.DELETE.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	
	-- delete client objects stream listing and the object
	stream.client.streams[stream.name] = nil
	stream = nil -- i know...
	return true, msg
end

function js.purge(stream, seq, subject, keep) -- support alt (subject, keep)
	if type(seq) == 'string' and not keep then subject, seq = seq, nil end
	local msg = stream.client:get(stream.api .. 'STREAM.PURGE.' .. stream.name, {filter=subject,seq=seq,keep=keep})
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.getmsg(stream, seq, subject)
	local msg = {}
	if type(seq) == 'string' and not subject then -- designed not to convert text as a way to assign a number to a subject
		subject = seq
		seq = nil
	end

	local data = { seq = seq }
	if subject then
		if data.seq and data.seq > 1 then
			data.next_by_subj = subject
		else
			data.last_by_subj = subject
			data.seq = nil
		end
	end

	if stream.config.allow_direct then
		if data.last_by_subj then
			msg = stream.client:get(stream.api .. format('DIRECT.GET.%s.%s', stream.name, subject))
		else
			msg = stream.client:get(stream.api .. format('DIRECT.GET.%s', stream.name), encode(data))
		end
	else
		msg = stream.client:get(stream.api .. format('STREAM.MSG.GET.%s', stream.name), encode(data))
	end
	
	return msg
end

function js.lastmsg(stream, subject) return js.getmsg(stream, 0, subject) end
function js.nextmsg(stream, subject) return js.getmsg(stream, nil, subject) end -- does this work?

function js.deletemsg(stream, seq, secure)
	local data = secure == true and {seq=seq} or {seq=seq,no_erase=true}
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, encode(data))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.publish(stream, m, opts) -- TODO broke but it sad 'publisher' 
	m = m or nats.msg(nil, nil, nil, nil, stream and stream.client)
	local header = decode_header(m.header)
	if opts.id then header['Nats-Msg-Id'] = opts.id end
	if opts.last_msg_id then header['Nats-Expected-Last-Msg-Id'] = opts.last_msg_id end
	if opts.stream then header['Nats-Expected-Stream'] = opts.stream end
	if opts.last_seq then header['Nats-Expected-Last-Sequence'] = opts.last_seq end
	if opts.last_subject_seq then header['Nats-Expected-Last-Subject-Sequence'] = opts.last_subject_seq end

	if opts.retry_wait then end
	if opts.retry_attempts then end
	if opts.wait then end
	
	local resp = m:get()
end

--
-- Consumers
local consumer_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex consumer: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
}
nats.jetstream.consumer_metatable = consumer_metatable
local con = consumer_metatable.methods -- consumer meta methods

local consumer_nuid = nats.nuid({preLen=8,seqLen=0})

local function consumer_update(consumer, data, stream)
	setmetatable(consumer, nil)
	consumer.config = data.config or consumer.config
	consumer.durable_name = data.durable_name or consumer.durable_name
	consumer.durable = data.durable_name and true or consumer.durable
	consumer.name = data.config.name or data.name or consumer.name
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
	consumer.sid = consumer.sid or cm.subscribe(stream.client, "_INBOX." .. stream.name .. "." .. data.config.name, function(m)
		print("got consumer sub msg")
		-- TODO add heartbeat support here as we will recieve the HB here.
		return m
	end)
	setmetatable(consumer, consumer_metatable)
	consumer.stream.consumers[consumer.name] = consumer
	return consumer
end
	
local function consumer_create(stream, data, opts) return consumer_update({}, data, stream) end

-- pullRequest struct
	-- Expires   time.Duration `json:"expires,omitempty"`
	-- Batch     int           `json:"batch,omitempty"`
	-- MaxBytes  int           `json:"max_bytes,omitempty"`
	-- NoWait    bool          `json:"no_wait,omitempty"`
	-- Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`

-- consumeOpts struct
	-- Expires                 time.Duration
	-- MaxMessages             int
	-- MaxBytes                int
	-- Heartbeat               time.Duration
	-- ErrHandler              ConsumeErrHandlerFunc
	-- ReportMissingHeartbeats bool
	-- ThresholdMessages       int
	-- ThresholdBytes          int
	-- StopAfter               int
	-- stopAfterMsgsLeft       chan int
	-- notifyOnReconnect       bool

function js.consumer(stream, config, opts, action)
	if action == nil and type(opts) == 'string' then
		action = opts
		opts = nil
	end
	config = type(config) == 'string' and {name=config} or config or {}
	opts = type(opts) == 'table' and opts or {}
	local msg = {}
	
	config = type(config) == 'table' and config or {}
	if config.durable and not config.durable_name then
		if type(config.durable) == 'string' and valid_consumer_name(config.durable) then config.durable_name = config.durable
		elseif config.durable == true and valid_consumer_name(config.name) then config.durable_name = config.name end
		return nil, 'invalid durable name'
	elseif config.durable_name then
		config.durable_name = valid_consumer_name(config.durable_name)
	end
	config.name = config.durable_name and nil
		or valid_consumer_name(config.name)
		or consumer_nuid()
	config.durable = config.durable_name and true

	local consumer = stream.consumers[config.name]
	if not consumer or action == 'info' then
		msg = stream.client:get(stream.api .. 'CONSUMER.INFO.' .. stream.name .. '.' .. config.name)
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then
			if action == 'info' or action == 'update' then return nil, 'consumer not found' end
		else
			consumer = consumer and consumer_update(consumer, msg.data, stream) or consumer_create(stream, msg.data, opts)
		end
	end
	
	if consumer and action == 'info' then return consumer
	else -- create (TODO: Should we combine configs for updates if we have a consumer?
		if valid_subject(config.filter_subject) then
			msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. config.name .. '.' .. config.filter_subject, encode({config=config,stream_name=stream.name,action=action}))
		else
			msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. (config.name and ('.' .. config.name) or ''), encode({config=config,stream_name=stream.name,action=action}))
		end
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return nil, msg end
		consumer = consumer and consumer_update(consumer, msg.data, stream) or consumer_create(stream, msg.data, opts)
	end
	return consumer, msg
end

function js.get_consumer(stream, name) return js.consumer(stream, name, 'info') end
function js.create_consumer(stream, config) return js.consumer(stream, config, 'create') end
function js.update_consumer(stream, config) return js.consumer(stream, config, 'update') end
function js.create_update_consumer(stream, config) return js.consumer(stream, config) end
function js.delete_consumer(stream, consumer_name) end
-- TODO paging support for information and not objects
function js.consumer_list(stream)
	local msg = stream.client:get(stream.api .. 'CONSUMER.LIST.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function con.delete(consumer)
	local stream = consumer.stream
	local msg = stream.client:get(stream.api .. 'CONSUMER.DELETE.' .. stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	-- remove assigned sub
	cm.unsubscribe(stream.client, consumer.sid)
	-- delete references
	stream.consumers[consumer.name] = nil
	return true, msg
end

function con.info(consumer)
	return js.consumer(consumer.stream, consumer.name, 'info')
end

function con.update(consumer, config, opts)
	return js.consumer(consumer.stream, config, opts, 'update')
end

function con.fetch(consumer, n, b, to, hb) -- int number, int bytes, int timeout, int hb
	local opts = {
		expires = tonumber(to) and to > 0 and to,
		batch = n > 1 and n or nil,
		max_bytes = tonumber(b),
		no_wait = to and to <= 0,
		idle_heartbeat = tonumber(hb),
	}
	local client = consumer.stream.client
	local msgs = {}
	for i = 1, n do
		local msg
		if i == 1 then msg = client:get(consumer.stream.api .. 'CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name, opts and encode(opts), nil, consumer.sid)
		else msg = cm.wait(client, {sid=consumer.sid,timeout=client.config.timeout}) end
		if msg then
			msg.data = decode(msg.data or json_error_true)
			if msg.data.error then return false, msg end
			insert(msgs, msg)
		else return msgs end
	end
	return msgs
end

function con.next(consumer, to, hb)
	return con.fetch(consumer, 1, nil, to, hb)[1]
end

function con.consume(consumer, n, opts) end
function con.messages(consumer, n, opts) end
function con.stop(consumer) end
function con.drain(consumer) end

-- Subscription for Stream -- consumer wrapper for new API?
--function js.subscribe(js, subject, func, queue, pull_opt, opts) end
function js.subscribe(js, subj, cb, config, queue, opts) -- isPollMode now opts.pull
	opts = type(opts) == 'table' and opts or {}
	config = type(config) == 'table' and config or {}
	config.deliver_policy = "deliverPolicyNotSet"
    config.ack_policy = "ackPolicyNotSet"
    config.replay_policy = "replayPolicyNotSet"

    -- Handling pull subscribers
    if opts.pull and config.deliver_subject then
        return nil, "ErrPullSubscribeToPushConsumer"
    end

    -- Handling queue subscriptions
    if queue then
        if config.idle_heartbeat or config.flow_control then
            return nil, "nats: queue subscription doesn't support idle heartbeat nor flow control"
        end

        -- Set durable name if needed -- this will not work with consumer as is.
        if not opts.consumer and not config.durable_name then
            local r = valid_consumer_name(queue)
            if not r then return nil, err end
            config.durable_name = queue
        end
    end

    -- Handle ordered consumers
    if opts.ordered then
        if config.durable_name then return nil, "nats: durable cannot be set for an ordered consumer" end
        if config.ack_policy ~= "ackPolicyNotSet" then return nil, "nats: ack policy cannot be set for an ordered consumer" end
        if config.max_deliver then return nil, "nats: max deliver cannot be set for an ordered consumer" end
        if config.deliver_subject then return nil, "nats: deliver subject cannot be set for an ordered consumer" end
        if queue then return nil, "nats: queues cannot be set for an ordered consumer" end
        if opts.consumer then return nil, "nats: cannot bind existing consumer for an ordered consumer" end
        if opts.pull then return nil, "nats: cannot use pull mode for an ordered consumer" end

        config.flow_control = true
        config.ack_policy = "AckNonePolicy"
        config.max_deliver = 1
        config.ack_wait = 22 * 60 * 60 -- 22 hours
        config.num_replicas = 1
        config.memory_storage = true
        config.idle_heartbeat = config.idle_heartbeat or 5 -- 5 seconds default
	end
	
	opts.consumer = opts.consumer or config.durable_name

    -- Process consumer information
    local info
    if opts.consumer ~= "" and not opts.skipCInfo then
        info, err = js:ConsumerInfo(stream, opts.consumer)
        if err then
            return nil, err
        end
    end

    -- Final setup for consumer creation
    if opts.pull then deliver = "new_inbox_string" end -- Replace with actual logic

    -- Create subscription object
    local jsi = {
        js = js,
        stream = stream,
        consumer = opts.consumer,
        deliver = deliver,
        hbi = config.Heartbeat,
        ordered = opts.ordered,
        pull = isPullMode,
        psubj = subj,
        cancel = function() end,
        ackNone = config.ack_policy == "AckNonePolicy"
    }

    -- Subscribe to the stream
    local sub, sub_err = nc:subscribe(deliver, queue, cb, ch, isSync, jsi)
    if sub_err then
        return nil, sub_err
    end

    return sub, nil
end

-- Message Support
-- TODO add publish replacement for stream handling
local function ack_msg(m, t, opt, ack) -- msg, type, opt(int delay, string term)
	--if not m then return false, "ErrMsgNotBound" -- this is stupid? check for subscription?
	if m.ackd then return false, "Message already acknolwedged."
	elseif not m.reply then return false, "ErrMsgNoReply" end

	if t then
		if t == "-NAK" and type(opt) == 'number' then t = concat{"-NAK {\"delay\": ", d*1000, "}"}
		elseif t == "+TERM" and type(opt) == 'string' then t = concat{t, " ", opt} end
		if t ~= "+WPI" then rawset(m, 'ackd', true) end -- TODO: Follow up and proof
	end
	
	if ack or t == "+NXT" then return m:get(m.reply, t)
	else return m:respond(t) end
end

function mm.acked(m) return m.ackd end -- stupid but points to ackd
function mm.ack(m, t, opt, ack) return ack_msg(m, t, opt, ack) end
function mm.ack_ack(m) return ack_msg(m, "+ACK", nil, true) end
mm.double_ack = mm.ack_ack
function mm.nak(m, d) return ack_msg(m, "-NAK", d) end
function mm.term(m, s) return ack_msg(m, "+TERM", s) end
function mm.in_progress(m) return ack_msg(m, "+WPI") end
mm.wpi = mm.in_progress
function mm.ack_next(m) return ack_msg(m, "+NXT") end

function mm.next(m) end -- consumer or stream next?
function mm.stop(m) end --?
function mm.drain(m) end --?

function mm.status(m) -- user messsages will not have a status and can be used as a check_msg
    if not m.header or not m.header.Status then return nil end -- do we really need to check data? or (m.data and #m.data > 0) 
	return tonumber(m.header.Status), m.header.Description
end

-- this is wrong as check_msg was for inbound from server and not general handling as pending is really in reply?
function mm.pending(m)
	if not m.header then return nil end
    return tonumber(m.header["Nats-Pending-Messages"]), tonumber(m.header["Nats-Pending-Bytes"])
end

local str_explode = function(s, ss, p)
    local t, i = {}, 1
    for st in gmatch(s, p and ss or "([^" .. ss .. "]+)") do
        t[i] = st
        i = i + 1
    end
    return t, i-1
end

function mm.metafields(m)
	if not m.reply then return {} end
	local s, tc = str_explode(m.reply, ".") -- string, token count
	if s[1] == "$JS" and s[2] == "ACK" then
		if tc == 12 then
			return {
				domain = s[3],
				account_hash = s[4],
				stream = s[5],
				consumer = s[6],
				delivered = tonumber(s[7]),
				stream_seq = tonumber(s[8]),
				consumer_seq = tonumber(s[9]),
				timestamp = tonumber(s[10]), -- convert to date string?
				pending = tonumber(s[11]),
				token = tonumber(s[12]),
			}
		elseif tc == 9 then
			return {
				--domain = "", -- duplicated behavior of go client
				--account_hash = "", -- same
				stream = s[3],
				consumer = s[4],
				delivered = tonumber(s[5]),
				stream_seq = tonumber(s[6]),
				consumer_seq = tonumber(s[7]),
				timestamp = tonumber(s[8]),
				pending = tonumber(s[9]),
			}
		end
	else return {} end
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



