local nats = {
	_VERSION	 = 'lua-nats-js 0.0.1',
	_DESCRIPTION = 'LUA client for NATS w/header, tls - https://nats.io',
	_COPYRIGHT   = 'Copyright (C) Sam Orlando 2023 - MIT Cheese Please',
	verbose = false, -- make client based
	network = {}, -- network functions
	request = {}, -- request handling (from client to server)
	response = {}, -- response handing (from server to client)
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
	version = '0.0.1',
}

local cjson  = require('cjson')
local uuid   = require('uuid')
local socket = require('socket')
local ssl = require('ssl')

-- ### Local Vars, Shortcuts, Speed
local network, response, request = nats.network, nats.response, nats.request -- Shortcuts
local verbose = nats.verbose -- (false or nil for off) levels 1-5
local insert, concat = table.insert, table.concat
local gettime = socket.gettime
local type = type
local floor, tonumber = math.floor, tonumber
local encode, decode = cjson.encode, cjson.decode

local client_metatable = nats.client_metatable -- !!!!!!

local valid_config_toggles = {headers=true,pedantic=true,tls_required=true,echo=true,no_responders=true,verbose=true}
local valid_config_strings = {user=true,pass=true,auth_token=true,name=true,sig=true,jwt=true,nkey=true,protocol=true}

local connect_defaults = {
	host = '127.0.0.1',
	port = 4222,
	tcp_nodelay = true,
	path = nil,
	timeout = -1,
	tls_required = false,
}

-- ### Local Functions
local function merge_connect_defaults(ct)
	ct = ct or {}
	for k, v in pairs(connect_defaults) do ct[k] = ct[k] or connect_defaults[k] end
	return ct
end

local function encode_headers(t)
	if type(t) == "table" then
		local tht = {}
		for k,v in pairs(t) do
			insert(tht, concat({k,': ',v,'\r\n'}))
		end
		return concat(tht)
	end
	return t or "" -- anything else? user messed up or compiled their own header string.
end
nats.encode_headers = encode_headers

local function decode_headers(s)
	local tbl = {}
	if not s then return end
	for line in s:gmatch("(.-)\r\n") do
		local k,v = line:match("(.-):%s*(.*)")
		if k then tbl[k] = v end
	end
	return tbl
end
nats.decode_headers = decode_headers

local function passthru_func(...) return unpack({...}) end
nats.passthru_func = passthru_func

local function create_client(config, n_opts)
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
			read   = nats.network.read,
			write  = nats.network.write,
			lread  = nil,
			lwrite = nil,
			opts = n_opts,
		},
		error = nats.error,
	}, client_metatable)

	for k,v in pairs(config or {}) do client.config[k] = v end	

	uuid.seed()
	return client
end

-- ### Network Functions (socket)
function network.write(client, buffer)
	if verbose then
		if verbose > 3 then print('->> '..buffer:sub(1,-3)) end
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
			if verbose > 3 then print('<<- '..line) end
		end
		return line
	end
end
local network_read = network.read

-- ### Response Methods / Ops
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

		data.payload = network_read(client) -- data_payload
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
		  repeat
			hread = network_read(client)
			dheaders = hread .. '\r\n' .. dheaders
		  until #hread == 0
		end
		data.headers = dheaders 

		data.payload = network_read(client) -- data_payload
		return data
	end,
	["+OK"] = function(s) return {op=s[1]} end,
}
local response_methods = response.methods

function response.read(client)
	local payload = network_read(client)
	if not payload then return end -- nonblocking move on
	
	local slices  = {}
	for slice in payload:gmatch('[^%s]+') do insert(slices, slice) end
	if slices[1] and response_methods[slices[1]] then
		return response_methods[slices[1]](slices, client)
	else
		return {op = "-ERR", payload = payload}
	end
end
local response_read = response.read

-- ### Request Functions
function request.write(client, buffer)
	local bufferType = type(buffer)
	if bufferType == 'string' then network_write(client, buffer)
	elseif bufferType == 'table' then network_write(client, concat(buffer))
	else client.error('argument error: ' .. bufferType) end
end
local request_write = request.write

-- ### Socket Functions and Connection
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

-- ### NATS Module Core Functions
function nats.error(message, level)
	error(message, (level or 1) + 1)
end

function nats.client(opts, host, port, timeout)
	local t_opts = {}

	if type(opts) == "string" then
		timeout, port, host, opts = port, host, opts, nil
	end
	
	for k,v in pairs(opts or {}) do
		if (valid_config_toggles[k] or valid_config_strings[k]) then t_opts[k] = v end
	end
	
	local n_opts = { host = host, port = port, timeout = tonumber(timeout), tls_required = t_opts.tls_required}
	
	return create_client(t_opts, n_opts)
end

function nats.verbose_level(v)
	if v ~= nil then
		nats.verbose = tonumber(v) or nil
		verbose = nats.verbose -- local reassign
	else return nats.verbose end
end

function nats.connect(...)
	local args, n_opts = {...}, {}

	if #args > 0 then
		local host, port, timeout = unpack(args)
		n_opts = { host = host, port = port, timeout = tonumber(timeout) }
	end

	local client = create_client(nil, n_opts)
	client:connect()
end

function nats.create_inbox() return '_INBOX.' .. uuid() end
local create_inbox = nats.create_inbox -- vroom

-- ### NATS Client Methods
local cm = nats.client_metatable.methods
function cm.connect(client)
	if client.connected and client.network.socket then return true end --review - how do we handle reconnect? do we need to redo all?

	client.network.socket = client.network.socket or create_connection(merge_connect_defaults(client.network.opts))
	if not client.network.socket then client.error("Unable to connect socket.") end
	
	request_write(client, {'CONNECT ', encode(client.config),'\r\n'})

	-- gather the server information
	local data = response_read(client)
	if data.op == 'INFO' then
		client.information = data.payload
	end
	client.connected = true
end

function cm.ping(client)
	request_write(client, 'PING\r\n')
	local data = response_read(client)
	if data.op == 'PONG' then return true end
	return false
end

function cm.pong(client) request_write(client, 'PONG\r\n') end

function cm.publish(client, subject, payload, reply, headers)
	local st
	payload = payload or ''
	
	if headers and #headers > 0 then
		st = {'HPUB ', subject}
	else
		st = {'PUB ', subject}
		headers = nil
	end
	
	-- if reply insert
	if reply then
		insert(st, ' ')
		insert(st, reply)
	end
	
	-- headers add size and to total size
	if headers then
		headers = 'NATS/1.0\r\n' .. headers .. '\r\n'
		insert(st, ' ')
		insert(st, #headers)
		insert(st, ' ')
		insert(st, (#headers + #payload))
		insert(st, '\r\n')
		-- insert the headers 
		insert(st, headers)
	else
		-- insert payload size
		insert(st, ' ')
		insert(st, #payload)
		insert(st, '\r\n')
	end

	-- insert payload
	insert(st, payload)
	insert(st, '\r\n')

	request_write(client, st)
end

function cm.request(client, subject, callback, payload, headers)
	local inbox = create_inbox() 
	local sid = cm.subscribe(client, inbox, function(payload, reply, subject, sid, headers)
		cm.unsubscribe(client, sid)
		if callback then
			local s, d = pcall(callback, payload, reply, subject, sid, headers)
			if s then return d end
		end
	end)

	cm.publish(client, subject, payload, inbox, headers)
	
	return sid
end

function cm.get(client, subject, payload, headers)
	local sid = cm.request(client, subject, nil, payload, headers)
	local data = cm.wait(client, sid, nil, 2)
	
	if data then
	else cm.unsubscribe(client, sid) end

	return data
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
	
	request_write(client, st)
	client.subscriptions[sid] = callback
	return sid
end

function cm.unsubscribe(client, sid)
	request_write(client, {'UNSUB ', sid, '\r\n'})
	client.subscriptions[sid] = nil
end

function cm.auth(client, user, pass)
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return {user=client.config.user,pass=client.config.pass}
end

function cm.server_info(client) return client.information end

nats.encoded_client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, "client")[key] end,
	methods = {}
}

function cm.encoded_client(client)
	return setmetatable({client = client}, nats.encoded_client_metatable)
end

local ec = nats.encoded_client_metatable.methods
function ec.publish(client, subject, payload, reply, headers)
	client = rawget(client, "client")
	_, payload = pcall(client.encoding.payload, payload)
	_, headers = pcall(client.encoding.headers, headers)
	cm.publish(client, subject, payload, reply, headers)
end

function ec.get(client, subject, payload, headers)
	client = rawget(client, "client")
	_, payload = pcall(client.encoding.payload, payload)
	_, headers = pcall(client.encoding.headers, headers)
	local data = cm.get(client, subject, payload, headers)
	_, data.payload = pcall(client.decoding.payload, data.payload)
	_, data.headers = pcall(client.decoding.headers, data.headers)
	return data
end

function ec.request(client, subject, callback, payload, headers)
	client = rawget(client, "client")
	_, payload = pcall(client.encoding.payload, payload)
	_, headers = pcall(client.encoding.headers, headers)
	return cm.request(client, subject, function(client, subject, callback, payload, headers)
		_, payload = pcall(client.decoding.payload, payload)
		_, headers = pcall(client.decoding.headers, headers)
		local s,r = pcall(callback, client, subject, callback, payload, headers)
		if s then return r end
	end, payload, headers)
end

function ec.subscribe(client, subject, callback, queue)
	cm.subscribe(getraw(client, "client"), subject, function(payload, reply, subject, sid, headers)
		_, payload = pcall(client.decoding.payload, payload)
		_, headers = pcall(client.decoding.headers, headers)
		local s,r = pcall(callback, payload, reply, subject, sid, headers)
		if s then return r end
	end, queue)
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

function cm.wait(client, sid, maxcount, timeout)
	local count, socket, data = 1, client.network.socket
	local timeout_b, timeout_t, last_timeout_b, last_timeout_t
	
	if not sid or type(sid) == "number" then 
		if maxcount then
			timeout = timeout or maxcount
		end
		maxcount = sid
		sid = nil
	end

	if timeout then
		local ttyp = type(timeout)
		if ttyp == "number" then timeout_b = timeout
		elseif ttyp == "table" then
			timeout_b = timeout[1]
			timeout_t = timeout[2]
		end

		last_timeout_b, last_timeout_t = socket:gettimeout()
		if timeout_b then socket:settimeout(timeout_b,'b') end
		if timeout_t then socket:settimeout(timeout_t,'t') end
	end
	
	local toe, loop_run_start = 0, gettime(), loop_run_start
	while true do
		data = response_read(client)
		if not data then return end
		local op = data.op
		
		-- add header decode and encode option
		if op == 'MSG' or op == 'HMSG' then
			for k,v in pairs(client.subscriptions[data.sid](data.payload, data.reply, data.subject, data.sid, data.headers) or {}) do
				data[k] = v
			end
			if sid and sid == data.sid then break end
			count = count + 1
			if maxcount and count >= maxcount then
				print("maxcount:" .. maxcount .. ":" .. count)
				break
			end
		elseif op == 'PING' then cm.pong(client)
		elseif op == 'INFO' then
			-- client.information = decode(data.payload) -- notsure if this will overwrite, so lets be safe...
			for k,v in pairs(decode(data.payload) or {}) do client.information[k] = v end
		end
		
		if timeout_b then
			toe = toe + 1
			if toe >= 100 then
				local current_time = gettime()
				if timeout_b <= (current_time - loop_run_start) then
					print("timeout on " .. count .. " nats:cm.wait loop " .. (current_time - loop_run_start))
					return
				end
				toe = 0	
			end
		end
	end
	
	if timeout_b then socket:settimeout(last_timeout_b,'b') end
	if timeout_t then socket:settimeout(last_timeout_t,'t') end

	return data
end

return nats