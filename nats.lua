local nats = {
	_VERSION	 = 'lua-nats 0.0.4',
	_DESCRIPTION = 'LUA client for NATS messaging system. https://nats.io',
	_COPYRIGHT   = 'Copyright (C) 2015 Eric Pinto',
	methods = {}, -- method metatable for nats core client object
	network = {}, -- network functions
	request = {}, -- request handling (to server)
	response = {}, -- response handing (from server)
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
	trace		 = false,  -- not my tick
	reconnect	 = true,
	subscriptions = {}, -- k:sid v:func
	information   = {}, -- server information from INFO in connect
}

-- ### Library requirements ###
local cjson  = require('cjson')
local uuid   = require('uuid')

-- ### Local properties / vars ###
local unpack = _G.unpack or table.unpack
local type = type
local encode, decode = cjson.encode, cjson.decode
local os_time, os_date = os.time, os.date
local floor = math.floor
local network, response, request = nats.network, nats.response, nats.request
local client_prototype, client_metatable = nats.client_prototype, nats.client_metatable

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

local function get_time()
	local currentTime = os_time()
	local cpuTime = os_clock()
	return currentTime + (cpuTime - floor(cpuTime))
end

local function create_client(socket)
	local client = setmetatable({}, client_metatable)
	for k,v in pairs(client_prototype) do rawset(client, k, v) end
	
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
	uuid.seed()
	return client
end


-- ### Network methods ###
function network.write(client, buffer)
	--if client.trace then print('->> '..buffer:sub(1,-3)) end
	print('->> '..buffer:sub(1,-3))
	local _, err = client.network.socket:send(buffer)
	if not err then client.network.lwrite = buffer
	else client.error(err) end
end
local network_write = network.write

function network.read(client, len)
	if len == nil then len = '*l' end
	local line, err = client.network.socket:receive(len)
	print('<<- '..line)
	--if client.trace then print('<<- '..line) end
	if not err then
		client.network.lread = line
		return line
	else client.error('connection error: ' .. err) end
end
local network_read = network.read

-- ### Response methods ###
response.methods = {
	INFO = function(s) return {action=s[1],content=s[2]} end,
	PING = function(s) return {action=s[1]} end,
	PONG = function(s) return {action=s[1]} end,
	MSG = function(s, client)
		local data = {
			action = s[1],
			subject = s[2],
			sid = s[3]
		}
		-- ask for line ending chars and remove them
		if #s == 4 then
			data.content = network_read(client, s[4]+2):sub(1, -3)
		else
			data.reply = s[4]
			data.content = network_read(client, s[5]+2):sub(1, -3)
		end
		return data
	end,
	["+OK"] = function(s) return {action=s[1]} end,
}
local response_methods = response.methods

function response.read(client)
	local payload = network_read(client)
	local slices  = {}
	local data	= {}

	for slice in payload:gmatch('[^%s]+') do table.insert(slices, slice) end

	if slices[1] and response_methods[slices[1]] then
		return response_methods[slices[1]](slices, client)
	else
		return {action = "-ERR", content = payload}
	end
end
local response_read = response.read


-- ### Request methods ###
-- Client request sender (RAW)
function request.raw(client, buffer)
	local bufferType = type(buffer)
	if bufferType == 'string' then network_write(client, buffer)
	elseif bufferType == 'table' then network_write(client, table.concat(buffer))
	else client.error('argument error: ' .. bufferType) end
end
local request_raw = request.raw


-- ### Socket connection methods ###
local function connect_tcp(socket, parameters)
	local host, port = parameters.host, tonumber(parameters.port)
	if parameters.timeout then socket:settimeout(parameters.timeout, 't') end

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
	if parameters.socket then return parameters.socket end
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
local cm = nats.client_metatable.methods
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

	request_raw(client, 'CONNECT '..encode(config)..'\r\n')

	-- gather the server information
	local data = response_read(client)
	if data.action == 'INFO' then
		client.information = decode(data.content)
	end
end

function cm.ping(client)
	request_raw(client, 'PING\r\n')
	local data = response_read(client)
	if data.action == 'PONG' then return true end
	return false
end

function cm.pong(client) request_raw(client, 'PONG\r\n') end

function cm.request(client, subject, payload, callback)
	local inbox = create_inbox()
	callback = callback or function() end
	local sid = client:subscribe(inbox, function(message, reply)
		client:unsubscribe(sid)
		callback(message, reply)
	end)
	client:publish(subject, payload, inbox)
	return sid, inbox
end

function cm.get(client, subject, payload)
	local inbox = create_inbox()
	local sid = cm.subscribe(client, inbox, function(message, reply) cm.unsubscribe(client, sid) end)
	cm.publish(client, subject, payload, inbox)
	local data = response_read(client)
	if data.action == '-ERR' then client.error('Client Error: ' .. (data.content or 'unknown error')) end
	return data.content
end

function cm.subscribe(client, subject, callback, queue)
	local sid = uuid()
	request_raw(client, 'SUB ' .. subject .. (queue and ' ' .. queue or '') .. ' ' .. sid .. '\r\n')
	client.subscriptions[sid] = callback
	return sid
end

function cm.unsubscribe(client, sid)
	request_raw(client, 'UNSUB '..sid..'\r\n')
	client.subscriptions[sid] = nil
end

function cm.publish(client, subject, payload, reply)
	payload = payload or ""
	reply = reply and ' ' .. reply or ''
	request_raw(client, {
		'PUB '..subject..reply..' '..#payload..'\r\n',
		payload..'\r\n',
		})
end

function cm.wait(client, maxcount)
	local count = 0
	
	while true do
		data = response_read(client)
		if data.action == 'MSG' then
			count = count + 1
			client.subscriptions[data.sid](data.content, data.reply, data.subject, data.sid)
		elseif data.action == 'PING' then cm.pong(client)
		elseif data.action == 'INFO' then
			-- client.information = decode(data.content) -- notsure if this will overwrite, so lets be safe...
			for k,v in pairs(decode(data.content) or {}) do client.information[k] = v end
		end
		if maxcount and count > maxcount then return end
	end
	return
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