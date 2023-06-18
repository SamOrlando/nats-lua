local nats = {
	_VERSION	 = 'lua-nats 0.1.0',
	_DESCRIPTION = 'LUA client for NATS messaging system. https://nats.io',
	_COPYRIGHT   = 'based off lua-nats 0.0.2 - Copyright (C) 2015 Eric Pinto',
	network = {}, -- network functions
	request = {}, -- request handling (to server)
	response = {}, -- response handing (from server)
}

-- todo - convert all string concat to tables
-- todo - header support and options
-- todo - make encoded connection a seperate object

nats.client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__call = function(self, ...) return	end,
	methods = {},
	lang = 'lua',
	version = nats._VERSION,
}

-- ### Library requirements ###
local cjson  = require('cjson')
local uuid   = require('uuid')

-- ### Local properties / vars ###
local unpack = _G.unpack or table.unpack
local type = type
local encode, decode = cjson.encode, cjson.decode
local floor = math.floor
local network, response, request = nats.network, nats.response, nats.request
local client_prototype, client_metatable = nats.client_prototype, nats.client_metatable

local valid_config_toggles = {verbose=true,headers=true,trace=true,pedantic=true,tls_required=true,echo=true,no_responders=true}
local valid_config_strings = {user=true,pass=true,auth_token=true,name=true,sig=true,jwt=true,nkey=true}

local connect_defaults = {
	host = '127.0.0.1',
	port = 4222,
	tcp_nodelay = true,
	path = nil,
	timeout = -1,
}

-- ### Local methods ###
local function merge_defaults(parameters)
	parameters = parameters or {}
	for k, v in pairs(connect_defaults) do parameters[k] = parameters[k] or connect_defaults[k] end
	return parameters
end

local function create_client(socket, config)
	local client = setmetatable({}, client_metatable)
	-- defaults
	client.config = {
		user = nil,
		pass = nil,
		lang = client_metatable.lang,
		version = client_metatable.version,
		verbose = false,
		pedantic = false,
		trace = false, -- make take out of config for speed considerations -- not offical - FIX
		reconnect = true, -- not used or documented as real client config value in connect
	}
	
	for k,v in pairs(config or {}) do client.config[k] = v end	

	client.subscriptions = {}
	client.information = {}
	
	-- en/decoder handler option for each client (local funcs for lazy)
	client.encoder = function(m) return m end -- default passthrough
	client.decoder = function(m) return m end -- default passthrough
	
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
	if client.config.trace then print('->> '..buffer:sub(1,-3)) end -- YUK! FIX THIS SOME HOW!
	local _, err = client.network.socket:send(client.encoder(buffer))
	if not err then client.network.lwrite = buffer
	else client.error(err) end
end
local network_write = network.write

function network.read(client, len)
	if len == nil then len = '*l' end
	local line, err = client.network.socket:receive(len)
	if client.config.trace then print('<<- '..(line or "")) end -- YUK # 2
	if not err then
		client.network.lread = line
		return line
	else client.error('connection error: ' .. err) end
end
local network_read = network.read

-- ### Response methods ###
response.methods = {
	INFO = function(s, client) return {action=s[1],content=s[2]} end, -- do not use client.encode
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
		return client.encoder(data)
	end,
	-- "NATS/1.0" : HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊
	HMSG = function(s, client)
		local data
		if #s > 5 then
			data = {
				action = s[1],
				subject = s[2],
				sid = s[3],
				reply = s[4],
				header_size = s[5],
				total_size = s[6],
			}
		else 
			data = {
				action = s[1],
				subject = s[2],
				sid = s[3],
				header_size = s[4],
				total_size = s[5],
			}
		end
		
		if network_read(client) == "NATS/1.0" then
			data.headers = {}
			local hcount, hread = 0
			while true do
				hread = network_read(client)
				if #hread == 0 then break end
				hcount = hcount + 1
				print(hread)	
			end
		end

		data.content = network_read(client)
		return client.encoder(data)
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

function nats.client(opts, host, port, timeout)
	local opt_t = {}

	if type(opts) == "string" then
		timeout = port
		port = host
		host = opts
		opts = nil
	end
	
	local nparms = { host = host, port = port, timeout = tonumber(timeout) }
	
	for k,v in pairs(opts or {}) do
		if (valid_config_toggles[k] or valid_config_strings[k]) then opt_t[k] = v end
	end
	
	local socket = create_connection(merge_defaults(nparms))
	return create_client(socket, opt_t)
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
	request_raw(client, 'CONNECT '..encode(client.config)..'\r\n')

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
		callback(client.decoder(message), reply)
	end)
	client:publish(subject, client.encoder(payload), inbox)
	return sid, inbox
end

function cm.get(client, subject, payload)
	local inbox = create_inbox()
	local sid = cm.subscribe(client, inbox, function(message, reply) cm.unsubscribe(client, sid) end)
	cm.publish(client, subject, client.encoder(payload), inbox)
	local data = response_read(client)
	if data.action == '-ERR' then client.error('Client Error: ' .. (data.content or 'unknown error')) end
	return client.decoder(data.content)
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
	payload = client.encoder(payload or "")
	reply = reply and ' ' .. reply or ''
	request_raw(client, {
		'PUB '..subject..reply..' '..#payload..'\r\n',
		payload..'\r\n',
		})
end

function cm.publish_whead(client, subject, payload, headers, reply)
	payload = client.encoder(payload or "")
	headers = headers or ""
	reply = reply and ' ' .. reply or ''
	request_raw(client, {
		'HPUB '..subject..reply..' '..#headers..' '..#payload..'\r\n',
		'\r\n\r\n',
		payload..'\r\n',
		})
end

-- HPUB <subject> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊

function cm.wait(client, maxcount)
	local count, data = 0
	
	while true do
		data = response_read(client)
		action = data.action
		
		if action == 'MSG' then
			count = count + 1
			client.subscriptions[data.sid](client.decoder(data.content), data.reply, data.subject, data.sid)
		elseif action == 'HMSG' then
			count = count + 1
			client.subscriptions[data.sid](client.decoder(data.content), data.reply, data.subject, data.sid, data.headers)
		elseif action == 'PING' then cm.pong(client)
		elseif action == 'INFO' then
			-- client.information = decode(data.content) -- notsure if this will overwrite, so lets be safe...
			for k,v in pairs(decode(data.content) or {}) do client.information[k] = v end
		end
		if maxcount and count > maxcount then return end
	end
	return
end

function cm.raw(client, buffer)
	request_raw(client, buffer .. '\r\n')
	return response_read(client)
end

function cm.auth(client, user, pass)
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return {user=client.config.user,pass=client.config.pass}
end

-- assign functions to handle bool options
local function config_set(client, k, v)
	local opt = valid_config_toggles[k] or valid_config_string[k]
	if opt then
		if v ~= nil then client.config[k] = v
		elseif valid_config_toggles[k] then client.config[k] = not client.config[k] end
		return client.config[k]
	end
	return
end

-- assign methods for all config toggle options
for k,v in pairs(valid_config_toggles) do
	cm[k] = cm[k] or function(client, b) return config_set(client, k, b) end
end
-- assign methods for all config string options
for k,v in pairs(valid_config_strings) do
	cm[k] = cm[k] or function(client, s) return config_set(client, k, s) end
end

function cm.shutdown(client) client.network.socket:shutdown() end

return nats