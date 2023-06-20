local nats = {
	_VERSION	 = 'lua-nats 0.1.2',
	_DESCRIPTION = 'LUA client for NATS messaging system. https://nats.io',
	_COPYRIGHT   = 'slightly based off Eric Pinto lua-nats 0.0.2 - Copyright (C) Sam Orlando 2023',
	network = {}, -- network functions
	request = {}, -- request handling (to server)
	response = {}, -- response handing (from server)
}

-- fix options to be pure in submit to server CONNECT
-- todo - make encoded connection a seperate object
-- todo - support verbose mode natively to nats-lua
-- todo - header encoding and decoding options
-- todo - protocol setting in CONNECT (confirm dynamic INFO adjustments in client)

nats.client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__call = function(self, ...) return	end,
	methods = {},
	lang = 'lua',
	version = '0.1.2',
}

local cjson  = require('cjson')
local uuid   = require('uuid')
local ssl = require('ssl')

-- ### Local properties / vars ###
local verbose = false -- (false or nil for off) levels 1-5
local insert, concat = table.insert, table.concat
local type = type
local floor, tonumber = math.floor, tonumber
local encode, decode = cjson.encode, cjson.decode
local network, response, request = nats.network, nats.response, nats.request
local client_prototype, client_metatable = nats.client_prototype, nats.client_metatable

local valid_config_toggles = {verbose=true,headers=true,pedantic=true,tls_required=true,echo=true,no_responders=true}
local valid_config_strings = {user=true,pass=true,auth_token=true,name=true,sig=true,jwt=true,nkey=true,protocol=true}

local connect_defaults = {
	host = '127.0.0.1',
	port = 4222,
	tcp_nodelay = true,
	path = nil,
	timeout = -1,
	tls_required = false,
}

-- ### Local methods ### -- fix for false boolean handling
local function merge_defaults(parameters)
	parameters = parameters or {}
	for k, v in pairs(connect_defaults) do parameters[k] = parameters[k] or connect_defaults[k] end
	return parameters
end

local function toHEX(s)
	local hexString, byteValue = ""
	for i = 1, #s do
		byteValue = string.byte(s, i)
		hexString = hexString .. string.format("%02X ", byteValue)
	end
	return hexString
end

local function create_client(socket, config)
	local client = setmetatable({}, client_metatable)
	-- defaults
	client.config = {
		user = nil,
		pass = nil,
		no_responders = false,
		verbose = false,
		protocol = 1,
		pedantic = false,
		tls_required = false,
	}
	
	for k,v in pairs(config or {}) do client.config[k] = v end	

	client.subscriptions = {}
	client.information = {}
	
	-- en/decoder handler option for each client (local funcs for lazy)
	client.encoder = function(m) return m end -- default passthrough
	client.decoder = function(m) return m end -- default passthrough
	
	if client.config.headers then
		client.header_encoder = function(m) return m end
		client.header_decoder = function(m) return m end
	end
	
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

local function encode_header(t) return concat({t[1],': ',t[2],'\r\n'}) end

local function decode_headers(s) return s:match("([^:]+):%s*(.+)") end

-- ### Network methods ###
function network.write(client, buffer)
	if verbose then
		if verbose > 0 then print('->> '..buffer:sub(1,-3)) end
		if verbose > 1 then print(toHEX(buffer)) end
	end
	local _, err = client.network.socket:send(client.encoder(buffer))
	if not err then client.network.lwrite = buffer
	else client.error(err) end
end
local network_write = network.write

function network.read(client, len)
	if len == nil then len = '*l' end
	local line, err = client.network.socket:receive(len)
	if verbose then
		if verbose > 0 then print('<<- '..line) end
		if verbose > 1 then print(toHEX(line)) end
	end
	if not err then
		client.network.lread = line
		return line
	else client.error('connection error: ' .. err) end
end
local network_read = network.read

-- ### Response methods ###
response.methods = {
	INFO = function(s, client) return {op=s[1],payload=s[2]} end,
	PING = function(s) return {op=s[1]} end,
	PONG = function(s) return {op=s[1]} end,
	MSG = function(s, client)
		local data
		if #s > 4 then
			data = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				reply = s[4],
				size = s[5],
			}
		else 
			data = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				size = s[4],
			}
		end
		
		data.payload = network_read(client)
		
		return data
	end,
	HMSG = function(s, client)
		local data
		if #s > 5 then
			data = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				reply = s[4],
				header_size = s[5],
				size = s[6],
			}
		else 
			data = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				header_size = s[4],
				size = s[5],
			}
		end
		
		local dheaders = ''
		if tonumber(data.header_size) > 0 and network_read(client) == "NATS/1.0" then
			local hread
			while true do
				hread = network_read(client)
				if #hread == 0 then
					data.headers = dheaders:sub(3)
					break
				else dheaders = dheaders .. '\r\n' .. hread end
			end
		end
		
		data.payload = network_read(client)
		return data
	end,
	["+OK"] = function(s) return {op=s[1]} end,
}
local response_methods = response.methods

function response.read(client)
	local payload = network_read(client)
	local slices  = {}
	local data	= {}

	for slice in payload:gmatch('[^%s]+') do insert(slices, slice) end

	if slices[1] and response_methods[slices[1]] then return response_methods[slices[1]](slices, client)
	else return {op = "-ERR", payload = payload} end
end
local response_read = response.read


-- ### Request methods ###
function request.raw(client, buffer)
	local bufferType = type(buffer)
	if bufferType == 'string' then network_write(client, buffer)
	elseif bufferType == 'table' then network_write(client, concat(buffer))
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

	if parameters.tls_required then
         local tls_params = {
            mode = "client",
            protocol = "tlsv1_2",
            options = "all",
         }
         local n_socket = ssl.wrap(socket, tls_params)
         assert(n_socket, 'failed to wrap socket with TLS')
		 n_socket:dohandshake()
		 socket = n_socket
    end
	
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
	
	for k,v in pairs(opts or {}) do
		if (valid_config_toggles[k] or valid_config_strings[k]) then opt_t[k] = v end
	end
	
	local nparms = { host = host, port = port, timeout = tonumber(timeout), tls_required = opt_t.tls_required}
	
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
	request_raw(client, {'CONNECT ',encode(client.config),'\r\n'})

	-- gather the server information
	local data = response_read(client)
	if data.op == 'INFO' then
		client.information = decode(data.payload)
	end
end

function cm.ping(client)
	request_raw(client, 'PING\r\n')
	local data = response_read(client)
	if data.op == 'PONG' then return true end
	return false
end

function cm.pong(client) request_raw(client, 'PONG\r\n') end

-- we may have to have handlers or a wait system that offers breakoffs
function cm.request(client, subject, payload, headers)
	local inbox = create_inbox()
	local sid = cm.subscribe(client, inbox, function(message, reply) cm.unsubscribe(client, sid) end)
	
	if headers then cm.h_publish(client, subject, client.encoder(payload), inbox, headers)
	else cm.publish(client, subject, client.encoder(payload), inbox, headers) end
	
	local data = response_read(client)
	if data.op == '-ERR' then client.error('Client Error: ' .. (data.payload or 'unknown error')) end
	return data
end

-- wait/read/sync issues? return?
function cm.get(client, subject, payload, headers)
	local inbox = create_inbox()
	local sid = cm.subscribe(client, inbox, function(message, reply) cm.unsubscribe(client, sid) end)
	
	if headers then cm.h_publish(client, subject, client.encoder(payload), inbox, headers)
	else cm.publish(client, subject, client.encoder(payload), inbox, headers) end
	
	local data = response_read(client)
	if data.op == '-ERR' then client.error('Client Error: ' .. (data.payload or 'unknown error')) end
	return client.decoder(data.payload)
end

function cm.subscribe(client, subject, callback, queue)
	local sid = uuid()
	
	local st = {'SUB ', subject, ' '}
	if queue then
		insert(st, queue)
		insert(st, ' ')
	end
	insert(st, sid)
	insert(st, '\r\n')
	
	request_raw(client, st)
	client.subscriptions[sid] = callback
	return sid
end

function cm.unsubscribe(client, sid)
	request_raw(client, {'UNSUB ', sid, '\r\n'})
	client.subscriptions[sid] = nil
end

function cm.publish(client, subject, payload, reply, headers)
	if headers then	return cm.h_publish(lient, subject, payload, reply, headers) end
	payload = client.encoder(payload or "")
	
	-- create send table for message string
	local st = {'PUB ', subject, ' '}

	if reply then
		insert(st, reply)
		insert(st, ' ')
	end

	insert(st, #payload)
	insert(st, '\r\n')
	
	insert(st, payload)
	insert(st, '\r\n')
	
	request_raw(client, st)
end

function cm.h_publish(client, subject, payload, reply, headers)
	payload = client.encoder(payload or '')

	if type(headers) == "table" then
		local tht = {'NATS/1.0\r\n'}
		for k,v in pairs(headers) do
			insert(tht, encode_header({k,v}))
		end
		insert(tht, '\r\n')
		headers = concat(tht)
	end
	-- create table for message string
	local st = {'HPUB ', subject, ' '}
	-- if reply insert
	if reply then
		insert(st, reply)
		insert(st, ' ')
	end
	-- header handling if nil
	if headers and #headers > 0 then
	else headers = '' end
	-- finish up the op string
	insert(st, #headers)
	insert(st, ' ')
	insert(st, (#payload + #headers))
	insert(st, '\r\n')
		-- insert headers
	insert(st, headers)
	-- insert payload
	insert(st, payload)
	insert(st, '\r\n')

	request_raw(client, st)
end

function cm.wait(client, maxcount)
	local count, data = 0
	
	while true do
		data = response_read(client)
		op = data.op
		
		if op == 'MSG' or op == 'HMSG' then
			count = count + 1
			client.subscriptions[data.sid](client.decoder(data.payload), data.reply, data.subject, data.sid, data.headers)
		elseif op == 'PING' then cm.pong(client)
		elseif op == 'INFO' then
			-- client.information = decode(data.payload) -- notsure if this will overwrite, so lets be safe...
			for k,v in pairs(decode(data.payload) or {}) do client.information[k] = v end
		end
		if maxcount and count > maxcount then return end
	end
	return
end

function cm.request_raw(client, buffer)
	request_raw(client, buffer)
	return response_read(client)
end

function cm.auth(client, user, pass)
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return {user=client.config.user,pass=client.config.pass}
end

function cm.server_info(client) return client.information end

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