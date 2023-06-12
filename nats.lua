local nats = {
	_VERSION	 = 'lua-nats 0.0.4',
	_DESCRIPTION = 'LUA client for NATS messaging system. https://nats.io',
	_COPYRIGHT   = 'Copyright (C) 2015 Eric Pinto',
	methods = {}, -- method metatable for nats core client object
	network = {},
	request = {},
	response = {},
}

nats.client_metatable = {
	methods = {},
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__call = function(self, ...) return	end,
}

nats.client_prototype = {
	user		  = nil,
	pass		  = nil,
	lang		  = 'lua',
	version	   = '0.0.4',
	verbose	   = false,
	pedantic	  = false,
	trace		 = false,
	reconnect	 = true,
	subscriptions = {},
	information   = {},
}

-- ### Library requirements ###
local cjson  = require('cjson')
local uuid   = require('uuid')

-- ### Local properties / vars ###
local unpack = _G.unpack or table.unpack
local network = nats.network
local response = nats.response
local request = nats.request
local client_metatable = nats.client_metatable

local defaults = {
	host		= '127.0.0.1',
	port		= 4222,
	tcp_nodelay = true,
	path		= nil,
}

-- ### Local methods ###
local function merge_defaults(parameters)
	parameters = parameters or {}
	for k, v in pairs(defaults) do
		parameters[k] = parameters[k] or defaults[k]
	end
	return parameters
end

local function create_client(socket)
	local client = setmetatable({}, client_metatable))
	for k, v in pairs(client_prototype) do client[k] = v end
	
	-- assign client error handler
	client.error = nats.error
	-- assign client network methods
	client.network = {
		socket = socket,
		read   = network.read,
		write  = network.write,
		lread  = nil,
		lwrite = nil,
	}
	-- -- assign client requests methods
	-- client.requests = {
		-- multibulk = request.raw,
	-- }
	uuid.seed()
	return client
end


-- ### Network methods ###

function network.write(client, buffer)
	if client.trace then print('->> '..buffer:sub(1,-3)) end
	local _, err = client.network.socket:send(buffer)
	if not err then
		client.network.lwrite = buffer
	else
		client.error(err)
	end
end
local network_write = network.write

function network.read(client, len)
	if len == nil then len = '*l' end
	local line, err = client.network.socket:receive(len)
	if client.trace then print('<<- '..line) end
	if not err then
		client.network.lread = line
		return line
	else
		client.error('connection error: ' .. err)
	end
end
local network_read = network.read

-- ### Response methods ###
-- Client response reader
function response.read(client)
	local payload = network_read(client)
	local slices  = {}
	local data	= {}

	for slice in payload:gmatch('[^%s]+') do
		table.insert(slices, slice)
	end

	-- PING
	if slices[1] == 'PING' then
		data.action = 'PING'

	-- PONG
	elseif slices[1] == 'PONG' then
		data.action = 'PONG'

	-- MSG
	elseif slices[1] == 'MSG' then
		data.action	= 'MSG'
		data.subject   = slices[2]
		data.unique_id = slices[3]
		-- ask for line ending chars and remove them
		if #slices == 4 then
			data.content   = network_read(client,  slices[4]+2):sub(1, -3)
		else
			data.reply	 = slices[4]
			data.content   = network_read(client,  slices[5]+2):sub(1, -3)
		end

	-- INFO
	elseif slices[1] == 'INFO' then
		data.action  = 'INFO'
		data.content = slices[2]

	-- INFO
	elseif slices[1] == '+OK' then
		data.action  = 'OK'

	-- INFO
	elseif slices[1] == '-ERR' then
		data.action  = 'ERROR'
		-- data.content = slices[2]

   -- unknown type of reply
	else
		data = client.error('unknown response: ' .. payload)
	end

	return data
end
local response_read = response.read


-- ### Request methods ###
-- Client request sender (RAW)
function request.raw(client, buffer)
	local bufferType = type(buffer)

	if bufferType == 'table' then
		network_write(client, table.concat(buffer))
	elseif bufferType == 'string' then
		network_write(client, buffer)
	else
		client.error('argument error: ' .. bufferType)
	end
end
local request_raw = request.raw


-- ### Socket connection methods ###
local function connect_tcp(socket, parameters)
	local host, port = parameters.host, tonumber(parameters.port)
	if parameters.timeout then
		socket:settimeout(parameters.timeout, 't')
	end

	local ok, err = socket:connect(host, port)
	if not ok then
		nats.error('could not connect to '..host..':'..port..' ['..err..']')
	end
	socket:setoption('tcp-nodelay', parameters.tcp_nodelay)
	return socket
end

local function connect_unix(socket, parameters)
	local ok, err = socket:connect(parameters.path)
	if not ok then
		nats.error('could not connect to '..parameters.path..' ['..err..']')
	end
	return socket
end

local function create_connection(parameters)
	if parameters.socket then
		return parameters.socket
	end

	local perform_connection, socket

	if parameters.scheme == 'unix' then
		perform_connection, socket = connect_unix, require('socket.unix')
		assert(socket, 'your build of LuaSocket does not support UNIX domain sockets')
	else
		if parameters.scheme then
			local scheme = parameters.scheme
			assert(scheme == 'nats' or scheme == 'tcp', 'invalid scheme: '..scheme)
		end
		perform_connection, socket = connect_tcp, require('socket').tcp
	end

	return perform_connection(socket(), parameters)
end

-- ### Nats library methods ###

function nats.error(message, level)
	error(message, (level or 1) + 1)
end

function nats.connect(...)
	local args, parameters = {...}, nil

	if #args == 1 then parameters = args[1]
	elseif #args > 1 then
		local host, port, timeout = unpack(args)
		parameters = { host = host, port = port, timeout = tonumber(timeout) }
	end

	local socket = create_connection(merge_defaults(parameters))
	return create_client(socket)
end

-- Create a properly formatted inbox subject.
function nats.create_inbox() return '_INBOX.' .. uuid() end
local create_inbox = nats.create_inbox -- vroom

-- ### NATS Client Methods ###
function cm.connect(client)
	local config = {
		lang	 = client.lang,
		version  = client.version,
		verbose  = client.verbose,
		pedantic = client.pedantic,
	}

	if client.user ~= nil and client.pass ~= nil then
		config.user = client.user
		config.pass = client.pass
	end

	request_raw(client, 'CONNECT '..cjson.encode(config)..'\r\n')

	-- gather the server information
	local data = response_read(client)
	if data.action == 'INFO' then
		client.information = cjson.decode(data.content)
	end
end

function cm.ping(client)
	request_raw(client, 'PING\r\n')

	-- wait for the server pong
	local data = response_read(client)
	if data.action == 'PONG' then
		return true
	else
		return false
	end

end

function cm.pong(client) request_raw(client, 'PONG\r\n') end

function cm.request(client, subject, payload, callback)
	local inbox = create_inbox()
	local unique_id = client:subscribe(inbox, function(message, reply)
		client:unsubscribe(unique_id)
		callback(message, reply)
	end)
	client:publish(subject, payload, inbox)
	return unique_id, inbox
end

function cm.subscribe(client, subject, callback, queue)
	local unique_id = uuid()
	if queue then request_raw(client, 'SUB '..subject..' '..queue..' '..unique_id..'\r\n')
	else request_raw(client, 'SUB '..subject..' '..unique_id..'\r\n') end
	client.subscriptions[unique_id] = callback

	return unique_id
end

function cm.unsubscribe(client, unique_id)
	request_raw(client, 'UNSUB '..unique_id..'\r\n')
	client.subscriptions[unique_id] = nil
end

function cm.publish(client, subject, payload, reply)
	reply = reply and ' ' .. reply or ''
	request_raw(client, {
		'PUB '..subject..reply..' '..#payload..'\r\n',
		payload..'\r\n',
	})
end

function cm.wait(client, quantity)
	quantity = quantity or 0

	local count = 0
	repeat
		local data = response_read(client)

		if data.action == 'PING' then
			cm.pong(client)

		elseif data.action == 'MSG' then
			count = count + 1
			client.subscriptions[data.unique_id](data.content, data.reply, data.subject, data.unique_id)
		end
	until quantity > 0 and count >= quantity
end

function cm.request_raw(client, buffer)
	request_raw(client, buffer .. '\r\n')
	return response_read(client)
end

function cm.set_auth(client, user, pass)
	client.user = user
	client.pass = pass
end

function cm.enable_trace(client) client.trace = true end

function cm.set_verbose(client, verbose) client.verbose = verbose end

function cm.set_pedantic(client, pedantic) client.pedantic = pedantic end

function cm.count_subscriptions(client) return #client.subscriptions end

function cm.server_info(client)	return client.information end

function cm.shutdown(client) client.network.socket:shutdown() end

return nats