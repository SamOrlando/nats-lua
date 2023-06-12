-- ### NATS-LUA Table
local nats = nats or require('nats')
nats.js = require('nats-jetstream')
local js = nats.js

-- ### Library requirements ###

local cjson  = require('cjson')
local uuid   = require('uuid')

-- ### Local properties ###
local unpack = _G.unpack or table.unpack
local request_raw = nats.request.raw
local response_read = nats.response.read

function js.create(client, stream_name, subjects)
	request_raw(client, 'PUB $JS.API.STREAM.CREATE.' .. stream_name .. ' \r\n' .. cjson.encode({ name = stream_name, subjects = subjects }) .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to create stream: ' .. (data.content or 'unknown error'))
	end
end

function js.delete(client, stream_name)
	request_raw(client, 'PUB $JS.API.STREAM.DELETE.' .. stream_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to delete stream: ' .. (data.content or 'unknown error'))
	end
end

function js.consumer.create(client, stream_name, consumer_name, config)
	request_raw(client, 'PUB $JS.API.CONSUMER.CREATE.' .. stream_name .. '.' .. consumer_name .. ' \r\n' .. cjson.encode(config) .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to create consumer: ' .. (data.content or 'unknown error'))
	end
end

function js.consumer.delete(client, stream_name, consumer_name)
	request_raw(client, 'PUB $JS.API.CONSUMER.DELETE.' .. stream_name .. '.' .. consumer_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to delete consumer: ' .. (data.content or 'unknown error'))
	end
end

function js.publish(client, stream_name, payload)
	request_raw(client, 'PUB ' .. stream_name .. ' \r\n' .. payload .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to publish message: ' .. (data.content or 'unknown error'))
	end
end

function js.info(client, stream_name)
	if stream_name then request_raw(client, 'PUB $JS.API.STREAM.INFO.' .. stream_name .. ' \r\n')
	else request_raw(client, 'PUB $JS.API.INFO \r\n') end
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to get stream api info: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.consumer.info(client, stream_name, consumer_name)
	request_raw(client, 'PUB $JS.API.CONSUMER.INFO.' .. stream_name .. '.' .. consumer_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to get consumer info: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.consumer.nextmsg(client, stream_name, consumer_name)
	request_raw(client, 'PUB $JS.API.CONSUMER.MSG.NEXT.' .. stream_name .. '.' .. consumer_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'MSG' then
		client.error('failed to get next message: ' .. (data.content or 'unknown error'))
	end
	return data.content -- you may want to parse this depending on your message format
end
js.nextmsg = js.consumer.nextmsg

function js.consumer.ack(client, stream_name, consumer_name, msg_sequence, delivery_sequence, domain)
	request_raw(client, 'PUB $JS.ACK.' .. stream_name .. '.' .. consumer_name .. '.' .. msg_sequence .. '.' .. delivery_sequence .. '.' .. domain .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to acknowledge message: ' .. (data.content or 'unknown error'))
	end
end

function js.ack(client, reply)
	request_raw(client, 'PUB ' .. reply .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to acknowledge message: ' .. (data.content or 'unknown error'))
	end
end

function js.streams(client)
	request_raw(client, 'PUB $JS.API.STREAM.LIST\r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to list streams: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.consumers(client, stream_name)
	request_raw(client, 'PUB $JS.API.CONSUMER.LIST.' .. stream_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to list consumers: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.update(client, stream_name, configuration)
	local configJson = cjson.encode(configuration)
	request_raw(client, 'PUB $JS.API.STREAM.UPDATE.' .. stream_name .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to update stream: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.consumer.update(client, stream_name, consumer_name, configuration)
	local configJson = cjson.encode(configuration)
	request_raw(client, 'PUB $JS.API.CONSUMER.UPDATE.' .. stream_name .. '.' .. consumer_name .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to update consumer: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.replay(client, stream_name, msg_sequence)
	request_raw(client, 'PUB $JS.API.STREAM.MSG.GET.' .. stream_name .. ' \r\n' .. msg_sequence .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'MSG' then
		client.error('failed to replay message: ' .. (data.content or 'unknown error'))
	end
	return data.content -- you may want to parse this depending on your message format
end

function js.remove(client, stream_name, msg_sequence)
	request_raw(client, 'PUB $JS.API.STREAM.MSG.DELETE.' .. stream_name .. ' \r\n' .. msg_sequence .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'MSG' then
		client.error('failed to delete stream message: ' .. (data.content or 'unknown error'))
	end
	return data.content -- you may want to parse this depending on your message format
end

function js.purge(client, stream_name)
	request_raw(client, 'PUB $JS.API.STREAM.PURGE.' .. stream_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to purge stream: ' .. (data.content or 'unknown error'))
	end
end

function js.template.create(client, template_name, config)
	request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.CREATE.' .. template_name .. ' \r\n' .. cjson.encode(config) .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to create stream template: ' .. (data.content or 'unknown error'))
	end
end

function js.template.delete(client, template_name)
	request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.DELETE.' .. template_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to delete stream template: ' .. (data.content or 'unknown error'))
	end
end

function js.template.info(client, template_name)
	request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.INFO.' .. template_name .. ' \r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to get stream template info: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.templates(client)
	request_raw(client, 'PUB $JS.API.STREAM.TEMPLATE.LIST \r\n')
	local data = response_read(client)
	if data.action ~= 'INFO' then
		client.error('failed to list stream templates: ' .. (data.content or 'unknown error'))
	end
	return cjson.decode(data.content)
end

function js.snapshot.create(client, stream_name, config)
	local reply = create_inbox() -- create_inbox is a function that generates a unique subject name
	local configJson = cjson.encode(config)
	request_raw(client, 'PUB $JS.API.STREAM.SNAPSHOT.' .. stream_name .. ' ' .. reply .. ' \r\n' .. configJson .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to create stream snapshot: ' .. (data.content or 'unknown error'))
	end
	return reply
end

--$JS.SNAPSHOT.RESTORE.<stream>.<msg id> future?
function js.snapshot.restore(client, stream_name, snap_data)
	local reply = client:subscribe(create_inbox(), function(message)
		-- handle any responses from the server
	end)
	request_raw(client, 'PUB $JS.API.STREAM.RESTORE.' .. stream_name .. ' ' .. reply .. ' ' .. #snap_data .. '\r\n' .. snap_data .. '\r\n')
	local data = response_read(client)
	if data.action ~= 'OK' then
		client.error('failed to restore stream: ' .. (data.content or 'unknown error'))
	end
	command.unsubscribe(client, reply) -- unsubscribe from the delivery subject
end

function js.snapshot.read(client, delievery_subject)
	local snapshot = {}
	local unique_id = client:subscribe(delievery_subject, function(message)
		table.insert(snapshot, message)
	end)
	command.wait(client) -- wait for all snapshot chunks to be received
	command.unsubscribe(client, unique_id) -- unsubscribe from the delivery subject
	return table.concat(snapshot) -- concatenate the chunks to form the complete snapshot
end

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