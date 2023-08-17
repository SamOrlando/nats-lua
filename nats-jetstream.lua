-- ### NATS-LUA Table
local nats = nats or require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
local uuid = require('uuid')

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode
local json_error_true = '{\"error\":true}'

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

-- add configuration option or shared subjects table option?
function cm.stream(client, stream_name, subjects)
	local nc = client.client or client -- encoded connections
	if subjects and type(subject) ~= "table" then subjects = {tostring(subjects)} end -- test and adjust subjects ; leaky part is 'true' being converted but meh.

	local msg = nc:get('$JS.API.STREAM.CREATE.' .. stream_name, encode({ name = stream_name, subjects = subjects }))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	-- create and set stream object
	local stream = setmetatable({
		name = stream_name,
		client = nc,
		subjects = subjects,
		consumers = {},
		state = msg.data.state or {},
		config = msg.data.config or {},
		cluster = msg.data.cluster or {},
	}, stream_metatable)

	-- insert stream info into client table
	rawset(nc, "streams", nc.streams or {})
	rawset(nc.streams, stream_name, stream.config)
	
	return stream, msg
end

function js.delete(stream) -- we will need two of them for cm and js
	-- make request
	local msg = stream.client:get('$JS.API.STREAM.DELETE.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	
	-- delete client objects stream listing and the object
	stream.client.streams[stream.name] = nil
	stream = nil
	return true, msg
end

function js.consumer(stream, consumer_name, config)
	local msg = stream.client:get('$JS.API.CONSUMER.CREATE.' .. stream.name .. '.' .. consumer_name, encode(config or {stream_name=stream.name,name=consumer_name}))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	-- create and set consumer object
	local consumer = setmetatable({
		name = consumer_name,
		stream = stream,
		config = msg.data.config,
		cluster = msg.data.cluster,
	}, consumer_metatable)

	-- insert consumer info into stream table and client 
	stream.consumers[consumer_name] = consumer.config
	stream.client.streams[stream.name].consumers = stream.client.streams[stream.name].consumers or {}
	stream.client.streams[stream.name].consumers[consumer_name] = consumer.config
	return consumer, msg
end

function con.delete(consumer)
	local msg = consumer.stream.client:get('$JS.API.CONSUMER.DELETE.' .. consumer.stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	-- delete references
	
	consumer.stream.consumers[consumer.name] = nil
	consumer.stream.client.streams[consumer.stream.name].consumers[consumer.name] = nil
	consumer = nil
	return true, msg
end

function js.publish(stream, subject, data, reply, header)
	local msg = stream.client:publish(subject, data, reply, header)
	return true, msg
end

function js.info(stream)
	local msg = stream.client:get('$JS.API.STREAM.INFO.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function con.info(consumer)
	local msg = consumer.stream.client:get('$JS.API.CONSUMER.INFO.' .. consumer.stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function con.nextmsg(consumer)
	local msg = consumer.stream.client:get('$JS.API.CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function cm.stream_list(client)
	local msg = client:get("$JS.API.STREAM.LIST")
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function js.consumer_list(stream)
	local msg = stream.client:get('$JS.API.CONSUMER.LIST.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function js.update(stream, configuration)
	local configJson = encode(configuration or {})
	local msg = stream.client:get('$JS.API.STREAM.UPDATE.' .. stream.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function con.update(consumer, configuration)
	local configJson = encode(configuration)
	local msg = consumer.client:get('$JS.API.CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.replay(stream, msg_sequence)
	local msg = stream.client:get('$JS.API.STREAM.MSG.GET.' .. stream.name, msg_sequence)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function js.remove(stream, msg_sequence)
	local msg = stream.client:get('$JS.API.STREAM.MSG.DELETE.' .. stream.name, msg_sequence)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.purge(stream)
	local msg = stream.client:get('$JS.API.STREAM.PURGE.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end