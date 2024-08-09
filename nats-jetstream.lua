-- TODO JetStream Consumer Idle Heartbeats - https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-9.md
-- TODO Prove Batch and Consumer Pull Options - https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-13.md


-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')

-- ### Local properties ### speed and neet laziness ftw
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local format, gmatch = string.format, string.gmatch
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
    if type(s) ~= 'string' or s == '' or s:find("[>*. /\\]") then return nil end
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
	stream.ts = data.ts or stream.ts or timestamp() -- TODO needed?
	setmetatable(stream, stream_metatable)
	rawset(stream.client, stream.name, stream)
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
	opts = opts or {}
	local msg = {}

	local client = client.client or client -- encoded connections and feels bad
	if not rawget(client, "streams") then rawset(client, "streams", {}) end -- insert stream info into client table -- TODO add hook for stream obj?
	
	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local stream = client.streams[scfg.name] -- lets find our stream
	if not stream or action == 'info' then -- if not, lets query server
		msg = client:get(stream_api .. 'STREAM.INFO.' .. scfg.name)
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
function cm.create_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'create') end
function cm.update_stream(client, scfg, opts) return cm.stream(client, scfg, opts, 'update') end
function cm.create_update_stream(client, scfg, opts) return cm.stream(client, scfg, opts) end
function cm.stream_by_subject() end
function cm.delete_stream() end
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
	local msg = ''
	if not subject and type(seq) == 'string' then -- designed not to convert text as a way to assign a number to a subject
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

function js.delmsg(stream, seq, secure)
	local data = secure == true and {seq=seq} or {seq=seq,no_erase=true}
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, encode(data))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
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
		return m
	end)
	setmetatable(consumer, consumer_metatable)
	stream.consumers[consumer.name] = consumer
	return consumer
end
	
local function consumer_create(stream, data, opts) return consumer_update({}, data, stream) end

function js.consumer(stream, config, opts, action)
	if action == nil and type(opts) == 'string' then
		action = opts
		opts = nil
	end
	config = type(config) == 'string' and {name=config} or config or {}
	opts = type(opts) == 'table' and opts or {}
	local msg = {}
	
	config = type(config) == 'table' and config or {}
	config.name = valid_consumer_name(config.name) or consumer_nuid()
	config.durable_name = config.durable_name or config.durable and config.name
	config.durable = nil

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

function con.nextmsg(consumer)
	local msg = consumer.stream.client:get(consumer.stream.api .. 'CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name, nil, nil, consumer.sid)
	if msg then
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return false, msg end
		return msg
	else return end
end

--
-- Message Support
local function ack_msg(m, t, d, ack) -- msg, type, delay, wait for ack?
	if m.ackd then return false, "Message already acknolwedged." end
	if t and t ~= "+WIP" then rawset(m, "ackd", true) end
	if d then t = (t or "-NAK") .. " {\"delay\": " .. ((tonumber(d) or 0)*1000) .. "}" end
	if t == "+NXT" or (ack and m.reply) then return m.new():get(m.reply, t) -- unsure what to do here...
	else return m:respond(t) end
end

function mm.ack(m) return ack_msg(m) end
function mm.ack_next(m) return ack_msg(m, "+NXT") end
function mm.ack_ack(m, rib) return ack_msg(m, "+ACK", nil, true) end
mm.double_ack = mm.ack_ack
function mm.nak(m, d) return ack_msg(m, "-NAK", d) end
function mm.term(m) return ack_msg(m, "+TERM") end -- TODO: Add with Reason The provided reason will be included in JetStream advisory event sent by the server.
function mm.in_progress(m) return ack_msg(m, "+WIP") end
mm.wip = mm.in_progress
function mm.next(m) end

local str_explode = function(s, ss, p)
    local t, i = {}, 1
    for st in gmatch(s, p and ss or "([^" .. ss .. "]+)") do
        t[i] = st
        i = i + 1
    end
    return t, i-1
end

function mm.metafields(m)
	local s, tc = str_explode(m.reply, ".") -- string, token count
	if s[1] == "$JS" and s[2] == "ACK" then
		if tc == 12 then
			return {
				domain = s[3],
				account_hash = s[4],
				stream = s[5],
				consumer = s[6],
				delivered = tonumber(s[7]),
				stream_sequence = tonumber(s[8]),
				consumer_sequence = tonumber(s[9]),
				timestamp = s[10],
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
				stream_sequence = tonumber(s[6]),
				consumer_sequence = tonumber(s[7]),
				timestamp = s[8],
				pending = tonumber(s[9]),
			}
		end
	else return {} end
end

return nats

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