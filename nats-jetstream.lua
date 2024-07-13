-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
local nuid = require('nuid')

-- ### Local properties ### speed and neet laziness ftw
local encode, decode = cjson.encode, cjson.decode
local encode_header, decode_header = nats.encode_header, nats.decode_header
local json_error_true = '{\"error\":true}'
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local format, gmatch = string.format, string.gmatch

-- copy go vars
local defaultAPIPrefix = "$JS.API."
local jsDomainT = "$JS.%s.API."
local jsExtDomainT = "$JS.%s.API"
local jsAPIPrefix = "%s."

local valid_subject = nats.valid_subject
function nats.valid_consumer_name(s)
    if type(s) ~= 'string' or s == '' or s:find("[>*. /\\]") then return nil end
    return s
end
local valid_consumer_name = nats.valid_consumer_name
nats.valid_stream_name = nats.valid_consumer_name
local valid_stream_name = nats.valid_stream_name



local stream_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex stream: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
}
nats.jetstream.stream_metatable = stream_metatable

local consumer_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex consumer: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
}
nats.jetstream.consumer_metatable = consumer_metatable

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client meta methods
local js = stream_metatable.methods -- stream meta methods
local con = consumer_metatable.methods -- consumer meta methods
local mm = nats.msg_metatable.methods -- msg meta methods 

function cm.stream(client, scfg, opts)
	if not scfg then return nil, 'no stream name provided' end
	if type(scfg) == 'string' then scfg = {name=scfg} end
	opts = opts or {}

	local nc = client.client or client -- encoded connections and feels bad
	if not rawget(nc, "streams") then rawset(nc, "streams", {}) end -- insert stream info into client table

	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	-- get existing stream info, update and/or create.
	local msg
	if opts.info_only then msg = nc:get(stream_api .. 'STREAM.INFO.' .. scfg.name)
	else msg = nc:get(stream_api .. 'STREAM.UPDATE.' .. scfg.name, encode(scfg)) end
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then
		if not opts.update_only and not opts.info_only then
			msg = nc:get(stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
			msg.data = decode(msg.data or json_error_true)
			if msg.data.error then return nil, msg end
		else return nil, msg end
	end
	
	local data = msg.data
	local stream = nc.streams[data.config.name] or {}
	setmetatable(stream, nil)
	
	-- assign base values
	stream.api = stream_api
	stream.name = data.config.name
	stream.client = nc
	stream.state = data.state
	stream.config = data.config
	stream.cluster = data.cluster
	stream.created = data.created
	stream.ts = data.ts

	-- fix me
	stream.consumers = stream.consumers or {} -- TODO FIX - Need to align existing consumer objects

	-- opts readjust or carry over
	stream.opts = stream.opts or {}
	local t_opts = stream.opts
	t_opts.pre = opts.pre and format(jsAPIPrefix, opts.pre) or t_opts.pre
	t_opts.maxpa = opts.maxpa or t_opts.maxpa or 4000 -- stupid var whats it for?
	t_opts.wait = opts.wait or t_opts.wait
	t_opts.aecb = nil
	t_opts.domain = t_opts.domain == false and nil or opts.domain
	t_opts.purge_opts = nil -- table,
	t_opts.stream_list_subject = nil --?
	t_opts.direct_get = nil
	t_opts.feature_flags = nil -- table
	setmetatable(stream, stream_metatable)
	
	-- TODO: MIRROR CHECK AND CONFIGURATION // If we have a mirror and an external domain, convert to ext.APIPrefix.
	-- TODO: SOURCE CHECK AND CONFIGURATION // Check sources for the same.
	-- TODO: // check that input subject transform (if used) is reflected in the returned StreamInfo
	
	nc.streams[stream.config.name] = stream
	return stream, msg
end

function cm.stream_list(client)
	local msg = client:get("$JS.API.STREAM.LIST")
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.update(stream, scfg, opts) -- update server and update obj
	opts = opts or {}
	opts.update_only = true
	return cm.stream(stream.client, scfg, opts)
end

function js.info(stream) -- get info from server and update obj
	return cm.stream(stream.client, nil, {info_only = true})
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

-- TODO Make all options possible.
-- // JSApiMsgGetRequest get a message request.
-- type JSApiMsgGetRequest struct {
	-- Seq     uint64 `json:"seq,omitempty"`
	-- LastFor string `json:"last_by_subj,omitempty"`
	-- NextFor string `json:"next_by_subj,omitempty"`

	-- // Batch support. Used to request more then one msg at a time.
	-- // Can be used with simple starting seq, but also NextFor with wildcards.
	-- Batch int `json:"batch,omitempty"`
	-- // This will make sure we limit how much data we blast out. If not set we will
	-- // inherit the slow consumer default max setting of the server. Default is MAX_PENDING_SIZE.
	-- MaxBytes int `json:"max_bytes,omitempty"`
	-- // Return messages as of this start time.
	-- StartTime *time.Time `json:"start_time,omitempty"`

	-- // Multiple response support. Will get the last msgs matching the subjects. These can include wildcards.
	-- MultiLastFor []string `json:"multi_last,omitempty"`
	-- // Only return messages up to this sequence. If not set, will be last sequence for the stream.
	-- UpToSeq uint64 `json:"up_to_seq,omitempty"`
	-- // Only return messages up to this time.
	-- UpToTime *time.Time `json:"up_to_time,omitempty"`
-- }

function js.lastmsg(stream, subject) return js.getmsg(stream, 0, subject) end
function js.nextmsg(stream, subject) return js.getmsg(stream, nil, subject) end -- does this work?

function js.delmsg(stream, seq, secure)
	local data = secure == true and {seq=seq} or {seq=seq,no_erase=true}
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, encode(data))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

local consumer_nuid = nats.nuid({preLen=8,seqLen=0})
function js.consumer(stream, consumer_name, config, action)
	if not action and type(consumer_name) == 'table' then
		action, config, consumer_name = config, consumer_name, config, nil
	end
	
	config = type(config) == 'table' and config or {}
	config.name = valid_consumer_name(consumer_name) or valid_consumer_name(config.name) or consumer_nuid()

	local msg
	if config.filter_subject or config.filter_subjects then -- fix me
		msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. config.name, encode({config=config,stream_name=stream.name,action=action}))
	else
		msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. config.name, encode({config=config,stream_name=stream.name,action=action}))
	end
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	local data = msg.data

	-- consumers assigned subscription
	local sid = cm.subscribe(stream.client, "_INBOX." .. stream.name .. "." .. data.name, function(m)
		print("got stream msg")
		return m
	end)

	-- create and set consumer object
	local consumer = setmetatable({
		name = data.name,
		stream = stream,
		config = data.config,
		cluster = data.cluster,
		sid = sid,
	}, consumer_metatable)

	-- insert consumer info into stream table and client 
	stream.consumers[consumer_name] = consumer.config
	stream.client.streams[stream.name].consumers = stream.client.streams[stream.name].consumers or {}
	stream.client.streams[stream.name].consumers[consumer_name] = consumer.config
	return consumer, msg
end

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
	stream.client.streams[stream.name].consumers[consumer.name] = nil
	return true, msg
end

function con.info(consumer)
	local msg = consumer.stream.client:get(consumer.stream.api .. 'CONSUMER.INFO.' .. consumer.stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function con.nextmsg(consumer)
	local msg = consumer.stream.client:get(consumer.stream.api .. 'CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name, nil, nil, consumer.sid)
	if msg then
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return false, msg end
		return msg
	else return end
end

function con.update(consumer, config)
	local configJson = encode(config)
	local msg = consumer.client:get(consumer.stream.api .. 'CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

-- modify and make msg objects jetstream enabled
local function ack_msg(m, t, d, ack) -- msg, type, delay, wait for ack?
	if m.ackd then return false, "Message already acknolwedged." end
	if t and t ~= "+WIP" then rawset(m, "ackd", true) end
	if d then t = (t or "-NAK") .. " {\"delay\": " .. ((tonumber(d) or 0)*1000) .. "}" end
	if t == "+NXT" or (ack and m.reply) then return m.new():get(m.reply, t) -- unsure what to do here...
	else return m:respond(t) end
end
function mm.is_ackd(m) return m.ackd end
function mm.ack(m) return ack_msg(m) end
function mm.ack_next(m) return ack_msg(m, "+NXT") end
function mm.ack_ack(m, rib) return ack_msg(m, "+ACK", nil, true) end
function mm.nak(m, d) return ack_msg(m, "-NAK", d) end
function mm.term(m) return ack_msg(m, "+TERM") end
function mm.in_progress(m) return ack_msg(m, "+WIP") end
mm.wip = mm.in_progress

local function str_explode(s, ss, p)
    if type(ss) ~= "string" or #ss == 0 then return error("Invalid separator.") end
    local t, i = {}, 1
    for sub in gmatch(s, p and ss or "([^" .. ss .. "]+)") do
        t[i] = sub
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
				timestamp = tonumber(s[10]),
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
				timestamp = tonumber(s[8]),
				pending = tonumber(s[9]),
			}
		end
	else return {} end
end

-- TODO mm.delete for removing in streams (js.remove)

return nats