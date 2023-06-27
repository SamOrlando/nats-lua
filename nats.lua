local nats = {
	_VERSION	 = 'lua-nats 0.1.2',
	_DESCRIPTION = 'LUA client for NATS messaging system. https://nats.io',
	_COPYRIGHT   = 'slightly based off Eric Pinto lua-nats 0.0.2 - Copyright (C) Sam Orlando 2023',
	verbose = false,
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
local verbose = nats.verbose -- (false or nil for off) levels 1-5
local insert, concat = table.insert, table.concat
local type = type
local floor, tonumber = math.floor, tonumber
local encode, decode = cjson.encode, cjson.decode
local network, response, request = nats.network, nats.response, nats.request
local client_metatable = nats.client_metatable

local valid_config_toggles = {headers=true,pedantic=true,tls_required=true,echo=true,no_responders=true}
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
local function merge_defaults(parms)
	parms = parms or {}
	for k, v in pairs(connect_defaults) do parms[k] = parms[k] or connect_defaults[k] end
	return parms
end

local function encode_headers(t)
	if type(t) == "table" then
		local tht = {'NATS/1.0\r\n'}
		for k,v in pairs(t) do
			insert(tht, concat({k,': ',v,'\r\n'}))
		end
		insert(tht, '\r\n')
		return concat(tht)
	end
	return t -- anything else? user messed up or compiled their own header string.
end

local function decode_headers(s)
	local tbl = {}
	for line in s:gmatch("(.-)\r\n") do
		local k,v = line:match("(.-):%s*(.*)")
		if k then tbl[k] = v end
	end
	return tbl
end
--s:match("([^:]+):%s*(.+)") end

local function passthru_func(...) return unpack({...}) end

local function toHEX(s) -- for debuging when js was mean on headers
	local hexString, byteValue = ""
	for i = 1, #s do
		byteValue = string.byte(s, i)
		hexString = hexString .. string.format("%02X ", byteValue)
	end
	return hexString
end

local function create_client(config, net_parms)
	-- defaults
	local client = setmetatable({
			connected = false,
			subscriptions = {},
			information = {},
			config = {
				user = nil,
				pass = nil,
				no_responders = false,
				verbose = false,
				protocol = 1,
				pedantic = false,
				tls_required = false,
			},
			network = {
				-- socket = socket,
				read   = network.read,
				write  = network.write,
				lread  = nil,
				lwrite = nil,
				parms = net_parms,
			},
			headers_encoder = encode_headers; -- if user wish to setup their own func, it will happen for MSG,HMSG,PUB
			headers_decoder = decode_headers; -- i added default header handling if table is passed. It will compile a table unless you set as passthru_func
			payload_encoder = passthru_func;
			payload_decoder = passthru_func;
			error = nats.error,
	}, client_metatable)
	
	for k,v in pairs(config or {}) do client.config[k] = v end	

	uuid.seed()
	return client
end


-- ### Network methods ###
function network.write(client, buffer)
	if verbose then
		if verbose > 0 then print('->> '..buffer:sub(1,-3)) end
		if verbose > 1 then print(toHEX(buffer)) end
	end
	local _, err = client.network.socket:send(buffer)
	if err then
		client.error('connection error: ' .. err)
	else client.network.lwrite = buffer end
end
local network_write = network.write

function network.read(client, len)
	if len == nil then len = '*l' end
	local line, err = client.network.socket:receive(len)
	if err then
		if err == "timeout" then return
		elseif err == "closed" then
			client.connected = false
			return
		end
		client.error('connection error: ' .. err)
	else
		client.network.lread = line
		if verbose then
			if verbose > 0 then print('<<- '..line) end
			if verbose > 1 then print(toHEX(line)) end
		end
		return line
	end
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
		
		data.payload = client.payload_decoder(network_read(client))
		
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
		
		-- need to add header decoder and encoder options
		local dheaders = ''
		if tonumber(data.header_size) > 0 and network_read(client) == "NATS/1.0" then
			local hread
			while true do
				hread = network_read(client)
				if #hread == 0 then break
				else dheaders = hread .. '\r\n' .. dheaders end
			end
		end
		
		printTable(client)
		data.headers = client.headers_decoder(dheaders) or dheaders 
		data.payload = client.payload_decoder(network_read(client))
		return data
	end,
	["+OK"] = function(s) return {op=s[1]} end,
}
local response_methods = response.methods

function response.read(client)
	local payload = network_read(client)
	if not payload then return end -- nonblocking move on
	
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
local function connect_tcp(socket, parms)
	local host, port = parms.host, tonumber(parms.port)
	if parms.timeout then socket:settimeout(parms.timeout, 't') end

	local ok, err = socket:connect(host, port)
	if not ok then
		nats.error('could not connect to '..host..':'..port..' ['..err..']')
	end

	socket:setoption('tcp-nodelay', parms.tcp_nodelay)

	if parms.tls_required then
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

local function connect_unix(socket, parms)
	local ok, err = socket:connect(parms.path)
	if not ok then
		nats.error('could not connect to '..parms.path..' ['..err..']')
	end
	return socket
end

local function create_connection(parms)
	local connect_func, socket

	if parms.unix then
		connect_func, socket = connect_unix, require('socket.unix')
		assert(socket, 'your build of LuaSocket does not support UNIX domain sockets')
	else
		connect_func, socket = connect_tcp, require('socket').tcp
	end
	
	return connect_func(socket(), parms)
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
	
	local network_parms = { host = host, port = port, timeout = tonumber(timeout), tls_required = opt_t.tls_required}
	
	return create_client(opt_t, network_parms)
end

function nats.verbose_level(v)
	if v ~= nil then
		nats.verbose = tonumber(v) or nil
		verbose = nats.verbose -- local reassign
	else return nats.verbose end
end

-- i hate this thing
function nats.connect(...)
	local args, parameters = {...}, nil

	if #args == 1 then parameters = args[1]
	elseif #args > 1 then
		local host, port, timeout = unpack(args)
		parameters = { host = host, port = port, timeout = tonumber(timeout) }
	end

	local client = create_client(socket, nil, parameters)
	client:connect()
end

-- Create a properly formatted inbox subject.
function nats.create_inbox() return '_INBOX.' .. uuid() end
local create_inbox = nats.create_inbox -- vroom

-- ### NATS Client Methods ###
local cm = nats.client_metatable.methods
function cm.connect(client)
	if not client.connected then 
		client.network.socket = create_connection(merge_defaults(client.network.parms))
		if not client.network.socket then client.error("Unable to connect socket.") end
	end
	
	request_raw(client, {'CONNECT ',encode(client.config),'\r\n'})

	-- gather the server information
	local data = response_read(client)
	if data.op == 'INFO' then
		client.information = data.payload
	end
	client.connected = true
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
	
	cm.publish(client, subject, client.payload_encoder(payload), inbox, client.headers_encoder(headers)) --header encoder?
	
	local data = response_read(client)
	if data.op == '-ERR' then client.error('Client Error: ' .. (data.payload or 'unknown error')) end
	
	return data
end

function cm.get(client, subject, payload, headers)
	local inbox = create_inbox()
	local sid = cm.subscribe(client, inbox, function() cm.unsubscribe(client, sid) end)

	cm.publish(client, subject, client.payload_encoder(payload), inbox, client.headers_encoder(headers))
		
	local data = response_read(client)
	if data.op == '-ERR' then client.error('Client Error: ' .. (data.payload or 'unknown error')) end
	return data.payload
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
	if headers then	return cm.h_publish(client, subject, payload, reply, headers) end
	payload = client.payload_encoder(payload or "")
	
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
	payload = client.payload_encoder(payload or '')
	headers = client.headers_encoder(headers or '')
	-- break out to headers decoder function
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

function cm.wait(client, maxcount, timeout_b, timeout_t)
	local count, socket, data = 0, client.network.socket
	local last_timeout_b, last_timeout_t
	
	while true do
		if timeout_b or timeout_t then
			last_timeout_b, last_timeout_t = socket:gettimeout()
			socket:settimeout(timeout_b,'b')
			if timeout_t then socket:settimeout(timeout_t,'t') end
		end

		data = response_read(client)
		if not data then return end
		
		op = data.op
		
		-- add header decode and encode option
		if op == 'MSG' or op == 'HMSG' then
			count = count + 1
			client.subscriptions[data.sid](data.payload, data.reply, data.subject, data.sid, data.headers)
		elseif op == 'PING' then cm.pong(client)
		elseif op == 'INFO' then
			-- client.information = decode(data.payload) -- notsure if this will overwrite, so lets be safe...
			for k,v in pairs(decode(data.payload) or {}) do client.information[k] = v end
		end
		
		if maxcount and count > maxcount then break end
	end
	
	socket:settimeout(last_timeout_b,'b')
	socket:settimeout(last_timeout_t,'t')

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