-- ### NATS-LUA Table
local nats = nats or require('nats')
nats.jetstream = {}

-- ### Library requirements ###

local cjson  = require('cjson')
local uuid   = require('uuid')

-- ### Local properties ###
local unpack = _G.unpack or table.unpack
local request_raw = nats.request.raw
local response_read = nats.response.read
local create_inbox = nats.create_inbox

local stream_prototype = {
	name = nil,
	client = nil,
	subjects = {},
	consumers = {},
}

local stream_metatable = {
	methods = {},
}
nats.jetstream.stream_metatable = stream_metatable

local consumer_prototype = {
	name = nil,
	client = nil,
	stream = {},
	config = nil,
}

local consumer_metatable = {
	methods = {},
}
nats.jetstream.consumer_metatable = consumer_metatable

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client
local js = stream_metatable.methods -- stream
local con = consumer_metatable.methods -- consumer

function js.create(client, stream_name, subjects)
	-- test and adjust subjects so string or table can be passed.
	-- Only leaky part is 'true' being converted but meh.
	if subjects then
		if type(subject) ~= "table" then subjects = {tostring(subjects)} end
	end
	
	-- request creation and 
	request_raw(client, 'PUB $JS.API.STREAM.CREATE.' .. stream_name .. ' \r\n' .. cjson.encode({ name = stream_name, subjects = subjects }) .. '\r\n')
	local data = response_read(client)
	if data.action == '-ERR' then
		client.error('failed to create stream: ' .. (data.content or 'unknown error'))
		return
	end

	-- create and set stream object
	local stream = setmetatable({}, stream_metatable)
	stream.name = stream_name
	stream.client = client
	stream.subjects = subjects
	
	-- insert stream info into client table
	client.streams = client.streams or {}
	client.streams[stream_name] = subjects
	
	return stream
end
-- client_method:jetstream('woot', {"woot.*","meep.tysx"})
cm.jetstream = js.create

function js.delete(stream) -- we will need two of them for cm and js
	request_raw(stream.client, 'PUB $JS.API.STREAM.DELETE.' .. stream.name .. ' \r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		stream.client.error('failed to delete stream: ' .. (data.content or 'unknown error'))
		return false
	end
	-- delete client objects stream listing and the object
	stream.client.streams[stream.name] = nil
	stream = nil
	return true
end

function con.create(stream, consumer_name, config)
	request_raw(stream.client, 'PUB $JS.API.CONSUMER.CREATE.' .. stream.name .. '.' .. consumer_name .. ' \r\n' .. cjson.encode(config) .. '\r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		client.error('failed to create consumer: ' .. (data.content or 'unknown error'))
		return
	end

	-- create and set consumer object
	local consumer = setmetatable({}, consumer_metatable)
	consumer.name = consumer_name
	consumer.client = client
	consumer.stream = stream
	consumer.config = config
	
	-- insert consumer info into stream table
	stream.consumers[consumer_name] = config
	
	return consumer
end
-- jetstream:consumer(consumer_name, config)
js.consumer = con.create

function con.delete(consumer)
	request_raw(consumer.client, 'PUB $JS.API.CONSUMER.DELETE.' .. consumer.stream.name .. '.' .. consumer.name .. ' \r\n')
	local data = response_read(consumer.client)
	if data.action == '-ERR' then
		consumer.client.error('failed to delete consumer: ' .. (data.content or 'unknown error'))
		return false
	end
	
	-- delete references
	consumer.stream[consumer.name] = nil
	consumer = nil
	return true
end

function js.publish(stream, payload)
	request_raw(stream.client, 'PUB ' .. stream.name .. ' \r\n' .. payload .. '\r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		stream.client.error('failed to publish message: ' .. (data.content or 'unknown error'))
		return false
	end
	return true
end

function js.info(stream)
	request_raw(stream.client, 'PUB $JS.API.STREAM.INFO.' .. stream.name .. ' \r\n')
	local data = response_read(stream.client)
	if data.action ~= 'INFO' then
		stream.client.error('failed to get stream api info: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function con.info(consumer)
	request_raw(consumer.client, 'PUB $JS.API.CONSUMER.INFO.' .. consumer.stream.name .. '.' .. consumer.name .. ' \r\n')
	local data = response_read(consumer.client)
	if data.action ~= 'INFO' then
		consumer.client.error('failed to get consumer info: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function con.nextmsg(consumer)
	request_raw(consumer.client, 'PUB $JS.API.CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name .. ' \r\n')
	local data = response_read(consumer.client)
	if data.action ~= 'MSG' then
		consumer.client.error('failed to get next message: ' .. (data.content or 'unknown error'))
		return
	end
	return data.content -- you may want to parse this depending on your message format
end

function con.ack(consumer, msg_sequence, delivery_sequence, domain)
	request_raw(consumer.client, 'PUB $JS.ACK.' .. consumer.stream.name .. '.' .. consumer.name .. '.' .. msg_sequence .. '.' .. delivery_sequence .. '.' .. domain .. ' \r\n')
	local data = response_read(consumer.client)
	if data.action == '-ERR' then
		con.client.error('failed to acknowledge message: ' .. (data.content or 'unknown error'))
		return false
	end
	return true
end

function js.ack(stream, reply)
	request_raw(stream.client, 'PUB ' .. reply .. ' \r\n')
	local data = response_read(client)
	if data.action == '-ERR' then
		stream.client.error('failed to acknowledge message: ' .. (data.content or 'unknown error'))
		return false
	end
	return true
end

function cm.stream_list(client)
	request_raw(client, 'PUB $JS.API.STREAM.LIST\r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to list streams: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function js.consumer_list(stream)
	request_raw(stream.client, 'PUB $JS.API.CONSUMER.LIST.' .. stream.name .. ' \r\n')
	local data = response_read(stream.client)
	if data.action ~= 'INFO' then
		stream.client.error('failed to list consumers: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function js.update(stream, configuration)
	local configJson = cjson.encode(configuration)
	request_raw(stream.client, 'PUB $JS.API.STREAM.UPDATE.' .. stream.name .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		stream.client.error('failed to update stream: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function con.update(consumer, configuration)
	local configJson = cjson.encode(configuration)
	request_raw(consumer.client, 'PUB $JS.API.CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(consumer.client)
	if data.action ~= 'INFO' then
		consumer.client.error('failed to update consumer: ' .. (data.content or 'unknown error'))
		return
	end
	return cjson.decode(data.content)
end

function js.replay(stream, msg_sequence)
	request_raw(stream.client, 'PUB $JS.API.STREAM.MSG.GET.' .. stream.name .. ' \r\n' .. msg_sequence .. '\r\n')
	local data = response_read(stream.client)
	if data.action ~= 'MSG' then
		stream.client.error('failed to replay message: ' .. (data.content or 'unknown error'))
		return
	end
	return data.content -- you may want to parse this depending on your message format
end

function js.remove(stream, msg_sequence)
	request_raw(stream.client, 'PUB $JS.API.STREAM.MSG.DELETE.' .. stream.name .. ' \r\n' .. msg_sequence .. '\r\n')
	local data = response_read(stream.client)
	if data.action ~= 'MSG' then
		stream.client.error('failed to delete stream message: ' .. (data.content or 'unknown error'))
		return
	end
	return data.content -- you may want to parse this depending on your message format
end

function js.purge(stream)
	request_raw(stream.client, 'PUB $JS.API.STREAM.PURGE.' .. stream.name .. ' \r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		stream.client.error('failed to purge stream: ' .. (data.content or 'unknown error'))
		return false
	end
	return true
end

js.snapshot = {}
function js.snapshot.create(stream, config)
	local delievery_subject = create_inbox() -- create_inbox is a function that generates a unique subject name
	local configJson = cjson.encode(config)
	request_raw(stream.client, 'PUB $JS.API.STREAM.SNAPSHOT.' .. stream.name .. ' ' .. delievery_subject .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		stream.client.error('failed to create stream snapshot: ' .. (data.content or 'unknown error'))
		return
	end
	return delievery_subject
end

--$JS.SNAPSHOT.RESTORE.<stream>.<msg id> future?
function js.snapshot.restore(stream, snap_data)
	local reply = stream.client:subscribe(create_inbox(), function(message)
		-- handle any responses from the server
	end)
	request_raw(stream.client, 'PUB $JS.API.STREAM.RESTORE.' .. stream.name .. ' ' .. #snap_data .. '\r\n' .. snap_data .. '\r\n')
	local data = response_read(stream.client)
	if data.action == '-ERR' then
		stream.client.error('failed to restore stream: ' .. (data.content or 'unknown error'))
		stream.client:unsubscribe(reply)
		return false
	end
	stream.client:unsubscribe(reply) -- unsubscribe from the delivery subject
	return true
end

function js.snapshot.read(stream, delievery_subject)
	local snapshot = {}
	local unique_id = stream.client:subscribe(delievery_subject, function(message)
		table.insert(snapshot, message)
	end)
	stream.client:wait() -- wait for all snapshot chunks to be received
	stream.client:unsubscribe(unique_id) -- unsubscribe from the delivery subject
	return table.concat(snapshot) -- concatenate the chunks to form the complete snapshot
end

-- function js.template.create(client, template_name, config)
	-- request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.CREATE.' .. template_name .. ' \r\n' .. cjson.encode(config) .. '\r\n')
	-- local data = response_read(client)
	-- if data.action == '-ERR' then
		-- client.error('failed to create stream template: ' .. (data.content or 'unknown error'))
	-- end
-- end

-- function js.template.delete(client, template_name)
	-- request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.DELETE.' .. template_name .. ' \r\n')
	-- local data = response_read(client)
	-- if data.action == '-ERR' then
		-- client.error('failed to delete stream template: ' .. (data.content or 'unknown error'))
	-- end
-- end

-- function js.template.info(client, template_name)
	-- request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.INFO.' .. template_name .. ' \r\n')
	-- local data = response_read(client)
	-- if data.action ~= 'INFO' then
		-- client.error('failed to get stream template info: ' .. (data.content or 'unknown error'))
	-- end
	-- return cjson.decode(data.content)
-- end

-- function js.templates(client)
	-- request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.LIST \r\n')
	-- local data = response_read(client)
	-- if data.action ~= 'INFO' then
		-- client.error('failed to list stream templates: ' .. (data.content or 'unknown error'))
	-- end
	-- return cjson.decode(data.content)
-- end

-- $JS.EVENT.METRIC.CONSUMER_ACK.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.STREAM.CREATED.<stream>
-- $JS.EVENT.ADVISORY.STREAM.DELETED.<stream>
-- $JS.EVENT.ADVISORY.STREAM.UPDATED.<stream>
-- $JS.EVENT.ADVISORY.CONSUMER.CREATED.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.CONSUMER.DELETED.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.STREAM.SNAPSHOT_CREATE.<stream>
-- $JS.EVENT.ADVISORY.STREAM.SNAPSHOT_COMPLETE.<stream>
-- $JS.EVENT.ADVISORY.STREAM.RESTORE_CREATE.<stream>
-- $JS.EVENT.ADVISORY.STREAM.RESTORE_COMPLETE.<stream>
-- $JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED.<stream>
-- $JS.EVENT.ADVISORY.STREAM.QUORUM_LOST.<stream>
-- $JS.EVENT.ADVISORY.CONSUMER.LEADER_ELECTED.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.CONSUMER.QUORUM_LOST.<stream>.<consumer>
-- $JS.EVENT.ADVISORY.API