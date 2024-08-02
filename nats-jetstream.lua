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

local function stream_create(client, scfg, opts) -- create new and return object with metatable attached.
	scfg, opts = scfg or {}, opts or {}
	if not scfg.name then return nil, 'name not defined' end
	
	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local stream = {
		api = stream_api,
		name = scfg.name,
		state = {},
		cluster = {},
		client = client,
		consumers = {}, -- place empty consumer table
		config = scfg,
		opts = {
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
	}
	setmetatable(stream, stream_metatable)
	
	client.streams[stream.name] = stream
	
	return stream
end

local function stream_get(client, scfg) -- checks if there is a local entry of the stream, if so return it.
	local scfg_type = type(scfg)
	scfg = scfg_type == 'string' and {name=scfg} or scfg_type == 'table' and scfg
	if client and scfg then return client.streams[scfg.name] end
end

function cm.stream(client, scfg, opts, action)
	if not scfg then return nil, 'no stream name provided' end
	if not action then
		if type(opts) == 'string' then
			action = opts
			opts = nil
		end
		if type(scfg) == 'string' then
			print('yea we did our thing')
			scfg = {name=scfg}
			action = 'info'
		end
	end
	opts = opts or {}
	local msg = {}

	local nc, ec = client.client or client, client.client -- encoded connections and feels bad
	if not rawget(nc, "streams") then rawset(nc, "streams", {}) end -- insert stream info into client table -- TODO add hook for stream obj?
	
	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local stream = stream_get(nc, scfg) -- lets find our stream
	if not stream or action == 'info' then -- if not, lets query server
		msg = nc:get(stream_api .. 'STREAM.INFO.' .. scfg.name)
		msg.data = decode(msg.data or json_error_true)
		stream = not msg.data.error and stream_create(nc, msg.data.config, opts) -- if no stream obj for this, lets make an object
	end
	
	if stream then
		if action == nil or action == 'update' then
			msg = nc:get(stream.api .. 'STREAM.UPDATE.' .. stream.name, encode(scfg))
			msg.data = decode(msg.data or json_error_true)
			if msg.data.error then return nil, msg end
		end
	elseif action == nil or action == 'create' then
		msg = nc:get(stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return nil, msg end
		stream = stream_create(nc, msg.data.config, opt)
	else
		return nil, 'stream not found'
	end

	-- TODO make this a function call as we may be doing it often?
	rawset(stream, 'state', msg.data.state)
	rawset(stream, 'cluster', msg.data.cluster)
	rawset(stream, 'ts', msg.data.ts)
	
	return stream, msg
end

-- TODO paging support for information and not objects
function cm.stream_list(client)
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

-- TODO Redo consumer to seperate info, update and create? Seems NATS decided to make stream and consumer functions ... not similar
local consumer_nuid = nats.nuid({preLen=8,seqLen=0})
function js.consumer(stream, consumer_name, config, action)
	if action == nil and type(consumer_name) == 'table' then
		action = config
		config = consumer_name
		consumer_name = nil
	end
	
	config = type(config) == 'table' and config or {}
	config.name = valid_consumer_name(consumer_name) or valid_consumer_name(config.name) or consumer_nuid()
	config.durable_name = config.durable_name or config.durable and config.name
	config.durable = nil

	local msg
	if valid_subject(config.filter_subject) then -- fix me
		msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. config.name .. '.' .. config.filter_subject, encode({config=config,stream_name=stream.name,action=action}))
	else
		msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. (config.name and ('.' .. config.name) or ''), encode({config=config,stream_name=stream.name,action=action}))
	end
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	local data = msg.data

	-- create and set consumer object
	local consumer = stream.consumers[data.config.name] or {}
	setmetatable(consumer, nil)
	consumer.name = data.config.name
	consumer.stream = stream
	consumer.config = data.config
	consumer.delivered = data.delivered
	consumer.ack_floor = data.ack_floor
	consumer.num_ack_pending = data.num_ack_pending
	consumer.num_redelivered = data.num_redelivered
	consumer.num_waiting = data.num_waiting
	consumer.num_pending = data.num_pending
	consumer.paused = data.paused
	consumer.pause_remaining = data.pause_remaining
	consumer.cluster = data.cluster
	consumer.ts = data.ts
	consumer.sid = consumer.sid or cm.subscribe(stream.client, "_INBOX." .. stream.name .. "." .. consumer.name, function(m)
		print("got stream msg")
		return m
	end)
	setmetatable(consumer, consumer_metatable)
	
	-- insert consumer info into stream table and client
	stream.consumers[consumer.name] = consumer

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
	return true, msg
end

function con.info(consumer) -- ?
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
	config = type(config) == 'table' and config or {}
	if config.durable_name or config.durable then
	else config.durable_name = consumer.config.durable_name end
	return js.consumer(consumer.stream, consumer.name, config, 'update')
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