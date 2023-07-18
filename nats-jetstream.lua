-- ### NATS-LUA Table
local nats = nats or require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
local uuid = require('uuid')

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode

local stream_metatable = {
	methods = {},
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__call = function(self, ...) return	end,
}
nats.jetstream.stream_metatable = stream_metatable

local consumer_metatable = {
	methods = {},
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__call = function(self, ...) return	end,
}
nats.jetstream.consumer_metatable = consumer_metatable

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client
local js = stream_metatable.methods -- stream
local con = consumer_metatable.methods -- consumer

-- add configuration option or shared subjects table option?
function cm.stream(client, stream_name, subjects)
	-- test and adjust subjects ; leaky part is 'true' being converted but meh.
	if subjects and type(subject) ~= "table" then subjects = {tostring(subjects)} end

	local msg = cm.get(client, '$JS.API.STREAM.CREATE.' .. stream_name, encode({ name = stream_name, subjects = subjects })).payload
	if msg.error then return false, msg end

	-- create and set stream object
	local stream = setmetatable({}, stream_metatable)
	stream.name = stream_name
	stream.client = client
	stream.subjects = subjects
	stream.consumers = {}
	
	-- insert stream info into client table
	client.streams = client.streams or {}
	client.streams[stream_name] = subjects
	
	return stream, msg
end

function js.delete(stream) -- we will need two of them for cm and js
	-- make request
	local msg = decode(stream.client:get('$JS.API.STREAM.DELETE.' .. stream.name).payload)
	if msg.error then return false, msg end
	
	-- delete client objects stream listing and the object
	stream.client.streams[stream.name] = nil
	stream = nil
	return true, msg
end

function js.consumer(stream, consumer_name, config)
	local msg = decode(stream.client:get('$JS.API.CONSUMER.CREATE.' .. stream.name .. '.' .. consumer_name, encode(config or {stream_name=stream.name,name=consumer_name})).payload)
	if msg.error then return false, msg end

	-- create and set consumer object
	local consumer = setmetatable({}, consumer_metatable)
	consumer.name = consumer_name
	consumer.client = stream.client
	consumer.stream = stream
	consumer.config = config
	
	-- insert consumer info into stream table
	stream.consumers[consumer_name] = config
	
	return consumer, msg
end

function con.delete(consumer)
	local msg = decode(consumer.client:get('$JS.API.CONSUMER.DELETE.' .. consumer.stream.name .. '.' .. consumer.name).payload)
	if msg.error then return false, msg end
	-- delete references
	consumer.stream[consumer.name] = nil
	consumer = nil
	return true, msg
end

function js.publish(stream, subject, payload)
	stream.client:publish(subject, payload)
	return true
end

function js.info(stream)
	local msg = decode(stream.client:get('$JS.API.STREAM.INFO.' .. stream.name).payload)
	if msg.error then return false, msg end
	return msg
end

function con.info(consumer)
	local msg = decode(consumer.client:get('$JS.API.CONSUMER.INFO.' .. consumer.stream.name .. '.' .. consumer.name).payload)
	if msg.error then return false, msg end
	return msg
end

function con.nextmsg(consumer)
	local msg = decode(consumer.client:get('$JS.API.CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name).payload)
	if msg.error then return false, msg end
	return msg
end

function cm.stream_list(client)
	local msg = decode(client:get("$JS.API.STREAM.LIST").payload)
	if msg.error then return false, msg end
	return msg
end

function js.consumer_list(stream)
	local msg = decode(stream.client:get('$JS.API.CONSUMER.LIST.' .. stream.name).payload)
	if msg.error then return false, msg end
	return msg
end

function js.update(stream, configuration)
	local configJson = encode(configuration)
	local msg = decode(stream.client:get('$JS.API.STREAM.UPDATE.' .. stream.name, configJson).payload)
	if msg.error then return false, msg end
	return true, msg
end

function con.update(consumer, configuration)
	local configJson = encode(configuration)
	local msg = decode(consumer.client:get('$JS.API.CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name, configJson).payload)
	if msg.error then return false, msg end
	return true, msg
end

function js.replay(stream, msg_sequence)
	local msg = decode(stream.client:get('$JS.API.STREAM.MSG.GET.' .. stream.name, msg_sequence).payload)
	if msg.error then return false, msg end
	return msg
end

function js.remove(stream, msg_sequence)
	local msg = decode(stream.client:get('$JS.API.STREAM.MSG.DELETE.' .. stream.name, msg_sequence).payload)
	if msg.error then return false, msg end
	return true, msg
end

function js.purge(stream)
	local msg = decode(stream.client:get('$JS.API.STREAM.PURGE.' .. stream.name).payload)
	if msg.error then return false, msg end
	return true, msg
end