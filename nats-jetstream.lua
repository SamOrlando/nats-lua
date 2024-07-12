-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
local nuid = require('nuid')

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode
local encode_header, decode_header = nats.encode_header, nats.decode_header
local json_error_true = '{\"error\":true}'
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local format = string.format

local defaultAPIPrefix = "$JS.API."
local jsDomainT = "$JS.%s.API."
local jsExtDomainT = "$JS.%s.API"
local jsAPIPrefix = "%s."

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
	-- test args and clear if needed
	opts = opts or {}
	if type(scfg) == 'string' then scfg = {name=scfg}
	elseif type(scfg) == 'table' and scfg.name then
	else return false, 'no stream name provided' end

	local nc = client.client or client -- encoded connections and feels bad

	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local msg = nc:get(stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	local data = msg.data
	local stream = setmetatable({
		api = stream_api,
		name = data.config.name,
		client = nc, -- we forcing base client as we are doing our own encoding with-in jetstream module
		consumers = {},
		state = data.state,
		config = data.config,
		cluster = data.cluster,
		created = data.created,
		ts = data.ts,
		opts = { -- this is sloppy alignment
			pre = opts.pre and format(jsAPIPrefix, opts.pre),
			maxpa = opts.maxpa or 4000, -- stupid var whats it for?
			wait = opts.wait,
			aecb = nil,
			domain = opts.domain,
			purge_opts = nil, -- table,
			stream_list_subject = nil, --?
			direct_get = nil,
			feature_flags = nil, -- table
		},
	}, stream_metatable)
	
	-- TODO: MIRROR CHECK AND CONFIGURATION // If we have a mirror and an external domain, convert to ext.APIPrefix.
	-- TODO: SOURCE CHECK AND CONFIGURATION // Check sources for the same.
	-- TODO: // check that input subject transform (if used) is reflected in the returned StreamInfo
	
	-- insert stream info into client table
	if not rawget(nc, "streams") then rawset(nc, "streams", {}) end
	nc.streams[stream.config.name] = stream
	
	return stream, msg
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

	-- // apiMsgDeleteT is the endpoint to remove a message.
	-- apiMsgDeleteT = "STREAM.MSG.DELETE.%s"

function js.getmsg(stream, seq, subject)
	local msg = ''
	if not subject and type(seq) == 'string' then
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

function js.consumer(stream, consumer_name, config)
	local msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. consumer_name, encode(config or {stream_name=stream.name,name=consumer_name}))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	-- consumers assigned subscription
	local sid = cm.subscribe(stream.client, "_INBOX." .. stream.name .. "." .. consumer_name, function(m)
		print("got stream msg")
		return m
	end)

	-- create and set consumer object
	local consumer = setmetatable({
		name = consumer_name,
		stream = stream,
		config = msg.data.config,
		cluster = msg.data.cluster,
		sid = sid,
	}, consumer_metatable)

	-- insert consumer info into stream table and client 
	stream.consumers[consumer_name] = consumer.config
	stream.client.streams[stream.name].consumers = stream.client.streams[stream.name].consumers or {}
	stream.client.streams[stream.name].consumers[consumer_name] = consumer.config
	return consumer, msg
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

function js.publish(stream, subject, data, reply, header)
	local msg = stream.client:publish(subject, data, reply, header)
	return true, msg
end

function js.info(stream)
	local msg = stream.client:get(stream.api .. 'STREAM.INFO.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
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

function cm.stream_list(client)
	local msg = client:get("$JS.API.STREAM.LIST")
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.consumer_list(stream)
	local msg = stream.client:get(stream.api .. 'CONSUMER.LIST.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.update(stream, config)
	local configJson = encode(config or {})
	local msg = stream.client:get(stream.api .. 'STREAM.UPDATE.' .. stream.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function con.update(consumer, config)
	local configJson = encode(config)
	local msg = consumer.client:get(consumer.stream.api .. 'CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.replay(stream, seq)
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.GET.' .. stream.name, seq)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function js.remove(stream, seq) -- msg:remove or delete()? conflict need to resolve in msg and own tables
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, seq)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.purge(stream)
	local msg = stream.client:get(stream.api .. 'STREAM.PURGE.' .. stream.name)
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

local gmatch = string.gmatch
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