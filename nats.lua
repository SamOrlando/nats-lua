local nats = {
	_VERSION	 = 'lua-nats-nx 0.0.1',
	_DESCRIPTION = 'LUA client for NATS w/header, tls - https://nats.io',
	_COPYRIGHT   = 'Copyright (C) Sam Orlando 2023 - MIT Cheese Please',
	verbose = false, -- make client based
	network = {}, -- network functions
	request = {}, -- request handling (from client to server)
	response = {}, -- response handing (from server to client)
	STALE_CONNECTION = 'stale connection',
	PERMISSIONS_ERR = 'permissions violation',
	AUTHORIZATION_ERR = 'authorization violation',
	AUTHENTICATION_EXPIRED_ERR = 'user authentication expired',
	AUTHENTICATION_REVOKED_ERR = 'user authentication revoked',
	ACCOUNT_AUTHENTICATION_EXPIRED_ERR = 'account authentication expired',
	MAX_CONNECTIONS_ERR = 'maximum connections exceeded',
	defaults = {
		host = '127.0.0.1',
		port = 4222,
		url = 'nats://127.0.0.1:4222',
		timeout = 2,
	},
}
nats.errors = {
		ConnectionClosed     = 'nats: connection closed',
		ConnectionDraining   = 'nats: connection draining',
		DrainTimeout         = 'nats: draining connection timed out',
		ConnectionReconnecting = 'nats: connection reconnecting',
		SecureConnRequired   = 'nats: secure connection required',
		SecureConnWanted     = 'nats: secure connection not available',
		BadSubscription      = 'nats: invalid subscription',
		TypeSubscription     = 'nats: invalid subscription type',
		BadSubject           = 'nats: invalid subject',
		BadQueueName         = 'nats: invalid queue name',
		SlowConsumer         = 'nats: slow consumer, messages dropped',
		Timeout              = 'nats: timeout',
		BadTimeout           = 'nats: timeout invalid',
		Authorization        = 'nats: authorization violation',
		AuthExpired          = 'nats: authentication expired',
		AuthRevoked          = 'nats: authentication revoked',
		AccountAuthExpired   = 'nats: account authentication expired',
		NoServers            = 'nats: no servers available for connection',
		JsonParse            = 'nats: connect message, json parse error',
		ChanArg              = 'nats: argument needs to be a channel type',
		MaxPayload           = 'nats: maximum payload exceeded',
		MaxMessages          = 'nats: maximum messages delivered',
		SyncSubRequired      = 'nats: illegal call on an async subscription',
		MultipleTLSConfigs   = 'nats: multiple tls.Configs not allowed',
		NoInfoReceived       = 'nats: protocol exception, INFO not received',
		ReconnectBufExceeded = 'nats: outbound buffer limit exceeded',
		InvalidConnection    = 'nats: invalid connection',
		InvalidMsg           = 'nats: invalid message or message nil',
		InvalidArg           = 'nats: invalid argument',
		InvalidContext       = 'nats: invalid context',
		NoDeadlineContext    = 'nats: context requires a deadline',
		NoEchoNotSupported   = 'nats: no echo option not supported by this server',
		ClientIDNotSupported = 'nats: client ID not supported by this server',
		UserButNoSigCB       = 'nats: user callback defined without a signature handler',
		NkeyButNoSigCB       = 'nats: nkey defined without a signature handler',
		NoUserCB             = 'nats: user callback not defined',
		NkeyAndUser          = 'nats: user callback and nkey defined',
		NkeysNotSupported    = 'nats: nkeys not supported by the server',
		StaleConnection      = 'nats: ' .. nats.STALE_CONNECTION,
		TokenAlreadySet      = 'nats: token and token handler both set',
		MsgNotBound          = 'nats: message is not bound to subscription/connection',
		MsgNoReply           = 'nats: message does not have a reply',
		ClientIPNotSupported = 'nats: client IP not supported by this server',
		Disconnected         = 'nats: server is disconnected',
		HeadersNotSupported  = 'nats: headers not supported by this server',
		BadHeaderMsg         = 'nats: message could not decode headers',
		NoResponders         = 'nats: no responders available for request',
		MaxConnectionsExceeded = 'nats: server maximum connections exceeded',
		ConnectionNotTLS     = 'nats: connection is not tls',
	}

--
-- todo - protocol setting in CONNECT (confirm dynamic INFO adjustments in client)
local cjson  = require('cjson')
local uuid   = require('uuid')
local socket = require('socket')
local ssl = require('ssl')

--
-- local vars, shortcuts, speed
local network, response, request = nats.network, nats.response, nats.request -- Shortcuts
local verbose = nats.verbose -- (false or nil for off) levels 1-5
local insert, concat = table.insert, table.concat
local gettime = socket.gettime
local type, pcall, rawget, setmetatable = type, pcall, rawget, setmetatable
local floor, tonumber = math.floor, tonumber
local encode, decode = cjson.encode, cjson.decode

--
-- list of valid configuration options
local valid_config_toggles = {headers=true,pedantic=true,tls_required=true,echo=true,no_responders=true,verbose=true}
local valid_config_strings = {user=true,pass=true,auth_token=true,name=true,sig=true,jwt=true,nkey=true,protocol=true}

--
-- built in lua native <-> http_header encoding and passthru function
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

function nats.passthru_func(...) return unpack({...}) end
local passthru_func = nats.passthru_func
nats.empty_func = function() end
local empty_func = nats.empty_func

--
-- nats client/conn object
nats.client_metatable = {
	__index = function(self, key) return rawget(self, key) or rawget(getmetatable(self), "methods")[key] or rawget(getmetatable(self), key) end,
	__newindex = function(self, key, value)
		local ev = rawget(self, key) -- existing value
		if ev ~= nil then ev = value
		else print("tried to set new/nil var: " .. key .. " on client object.") end
	end,
	__call = function(self, ...) return	end,
	methods = {},
	is_client = true,
	lang = 'lua',
	version = '0.0.1',
}
local client_metatable = nats.client_metatable -- expose for other extended modules (jetstream)

local function create_client(config, n_opts) -- fix default checks
	local client = setmetatable({
		connected = false,
		subscriptions = {},
		server_information = {},
		config = {
			reconnect = nats.defaults.reconnect and nats.defaults.reconnect or true,
			max_reconnect = nats.defaults.max_reconnect or 60,
			reconnect_wait = nats.defaults.reconnect_wait or 2,
			reconnect_jitter = nats.defaults.reconnect_jitter or .1,
			reconnect_jitter_tls = nats.defaults.reconnect_jitter_tls or 1,
			timeout = nats.defaults.timeout or 2,
			max_chan_len = nats.defaults.max_chan_len or (64 * 1024),
			tcp_nodelay = nats.defaults.tcp_nodelay and nats.defaults.tcp_nodelay or true,
			user = nats.defaults.user,
			pass = nats.defaults.pass,
			no_responders = nats.defaults.no_responders and nats.defaults.no_responders or false,
			verbose = nats.defaults.verbose or false,
			protocol = nats.defaults.protocol or 1,
			pedantic = nats.defaults.pedantic or false,
			tls_required = nats.defaults.tls_required or false,
			headers = nats.defaults.headers and nats.defaults.headers or true,
			echo = nats.defaults.echo or false,
			auth_token = nats.defaults.auth_token,
			name = nats.defaults.name,
			sig = nats.defaults.sig,
			jwt = nats.defaults.jwt,
			nkey = nats.defaults.nkey,
		},
		servers = {}, -- for expanding servers found
		network = {
			read   = nats.network.read,
			write  = nats.network.write,
			lread  = nil,
			lwrite = nil,
			opts = n_opts or {
				host = nats.defaults.host,
				port = nats.defaults.port,
				timeout = nats.defaults.tcp_timeout or -1,
				tls_required = nats.defaults.tls_required,
			},
		},
		error = nats.error,
	}, client_metatable)

	local conf_t = rawget(client, "config")
	for k,v in pairs(config or {}) do conf_t[k] = v end	

	uuid.seed()
	return client
end

--
-- nats msg object
nats.msg_metatable = {
	__index = function(self, key) return rawget(self, key) or rawget(getmetatable(self), "methods")[key] or rawget(getmetatable(self), key) end,
	__call = function(self, ...) return	end,
	methods = {},
	is_msg = true,
}
local msg_metatable = nats.msg_metatable

local function create_msg(mt)
	return setmetatable(mt, nats.msg_metatable)
end


function nats.is_msg(m)
	if type(m) == "table" and rawget(getmetatable(m), "is_msg") then return m end
	return nil
end
local is_msg = nats.is_msg

--
-- network methods
function network.write(client, buffer)
	if verbose then
		if verbose > 3 then print('->> '..buffer:sub(1,-3)) end
	end
	local _, err = client.network.socket:send(buffer)
	if err then
		client.connected = false
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

--
-- network socket functions
local function connect_tcp(socket, parms)
	local host, port = parms.host, tonumber(parms.port)
	if parms.timeout then socket:settimeout(parms.timeout, 't') end

	local ok, err = socket:connect(host, port)
	if not ok then
		nats.error('could not connect to '..host..':'..port..' ['..err..']')
	end

	if parms.tcp_nodelay ~= nil then socket:setoption('tcp-nodelay', parms.tcp_nodelay) end

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

--
-- response handling methods (from server)
response.methods = {
	INFO = function(s, client) return {op=s[1],data=s[2]} end,
	PING = function(s) return {op=s[1]} end,
	PONG = function(s) return {op=s[1]} end,
	MSG = function(s, client)
		local msg_t
		if #s > 4 then
			msg_t = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				reply = s[4],
				size = s[5],
			}
		else 
			msg_t = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				size = s[4],
			}
		end

		msg_t.data = network_read(client)
		return msg_t
	end,
	HMSG = function(s, client)
		local msg_t
		if #s > 5 then
			msg_t = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				reply = s[4],
				header_size = s[5],
				size = s[6],
			}
		else 
			msg_t = {
				op = s[1],
				subject = s[2],
				sid = s[3],
				header_size = s[4],
				size = s[5],
			}
		end
		
		local dheaders = ''
		if tonumber(msg_t.header_size) > 0 and network_read(client) == "NATS/1.0" then
		  local hread
		  repeat
			hread = network_read(client)
			dheaders = hread .. '\r\n' .. dheaders
		  until #hread == 0
		end
		msg_t.header = dheaders 

		msg_t.data = network_read(client)
		return msg_t
	end,
	["+OK"] = function(s) return {op=s[1],data=s[2]} end,
	["-ERR"] = function(s) return {op=s[1],data=s} end,
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
		return {op = "-ERR", data = payload}
	end
end
local response_read = response.read

--
-- request handling methods (to server)
function request.write(client, buffer)
	local bufferType = type(buffer)
	if bufferType == 'string' then network_write(client, buffer)
	elseif bufferType == 'table' then network_write(client, concat(buffer))
	else client.error('argument error: ' .. bufferType) end
end
local request_write = request.write

--
-- nats module core functions
function nats.error(message, level)
	error(message, (level or 1) + 1)
end

function nats.client(opts, host, port, timeout)
	local c_opts = {}

	if type(opts) == "string" then
		timeout, port, host, opts = port, host, opts, nil
	end
	
	-- scrub scrub!
	for k,v in pairs(opts or {}) do
		if (valid_config_toggles[k] or valid_config_strings[k]) then c_opts[k] = v end
	end
	
	local n_opts = {
				host = host or nats.defaults.host,
				port = port or nats.defaults.port,
				timeout = timeout or nats.defaults.tcp_timeout or -1,
				tls_required = nats.defaults.tls_required,
			}
	
	return create_client(c_opts, n_opts)
end

function nats.verbose_level(v)
	if v ~= nil then
		nats.verbose = tonumber(v) or nil
		verbose = nats.verbose -- local reassign
	else return nats.verbose end
end

function nats.connect(...) -- i hate this thing - please rewrite it!
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

--
-- nats client/conn methods
local cm = nats.client_metatable.methods
function cm.connect(client)
	if client.connected and client.network.socket then return true end --review - how do we handle reconnect? do we need to redo all?

	client.network.socket = client.network.socket or create_connection(client.network.opts)
	if not client.network.socket then client.error("Unable to connect socket.") end
	
	request_write(client, {'CONNECT ', encode(client.config),'\r\n'}) -- breakout to only needed data
		-- Verbose      bool   `json:"verbose"`
		-- Pedantic     bool   `json:"pedantic"`
		-- UserJWT      string `json:"jwt,omitempty"`
		-- Nkey         string `json:"nkey,omitempty"`
		-- Signature    string `json:"sig,omitempty"`
		-- User         string `json:"user,omitempty"`
		-- Pass         string `json:"pass,omitempty"`
		-- Token        string `json:"auth_token,omitempty"`
		-- TLS          bool   `json:"tls_required"`
		-- Name         string `json:"name"`
		-- Lang         string `json:"lang"`
		-- Version      string `json:"version"`
		-- Protocol     int    `json:"protocol"`
		-- Echo         bool   `json:"echo"`
		-- Headers      bool   `json:"headers"`
		-- NoResponders bool   `json:"no_responders"`

	-- gather the server information
	local msg_t = response_read(client)
	if msg_t.op == 'INFO' then
		client.server_information = msg_t.data
	end
	client.connected = cm.ping(client)
end

function cm.ping(client)
	request_write(client, 'PING\r\n')
	local msg_t = response_read(client)
	if msg_t.op == 'PONG' then return true end
	return false
end

function cm.pong(client) request_write(client, 'PONG\r\n') end

function cm.publish(client, subject, data, reply, header)
	local st -- send_table to request_write
	local msg = is_msg(subject) or create_msg({subject=subject, data=data, reply=reply, header=header, sid=sid, client=client}) -- confirm or create msg object
	header = msg.header -- local for multiple calls - set from msg
	data = msg.data or '' -- local for multiple and placing '' if nil - set from msg
	
	if header and #header > 0 then
		st = {'HPUB ', msg.subject}
	else
		st = {'PUB ', msg.subject}
		header = nil
	end
	
	-- if reply insert
	if msg.reply then
		insert(st, ' ')
		insert(st, msg.reply)
	end
	
	-- header add size and to total size
	if header then
		header = 'NATS/1.0\r\n' .. header .. '\r\n'
		insert(st, ' ')
		insert(st, #header)
		insert(st, ' ')
		insert(st, (#header + #data))
		insert(st, '\r\n')
		-- insert the header 
		insert(st, header)
	else
		-- insert data size
		insert(st, ' ')
		insert(st, #data)
		insert(st, '\r\n')
	end

	-- insert data
	insert(st, data)
	insert(st, '\r\n')

	request_write(client, st)
end

function cm.request(client, subject, callback, data, header)
	local msg = is_msg(subject) or create_msg({subject=subject, reply=create_inbox(), data=data, header=header, client=client})
	msg.sid = cm.subscribe(client, msg.reply, function(m)
		cm.unsubscribe(client, m.sid)
		if callback then
			local s, rm = pcall(callback, m)
			if s then return rm end
		end
	end)

	cm.publish(client, msg)

	return msg
end

function cm.get(client, subject, data, header)
	local msg = is_msg(subject) or create_msg({subject=subject, data=data, header=header, client=client})
	msg.sid = cm.request(client, msg)
	local r_msg = cm.wait(client, msg.sid, nil, nats.defaults.timeout) -- add timeout config here
	return r_msg or cm.unsubscribe(client, sid) -- return results or unsubscribe as its too late
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
	client.subscriptions[sid] = {subject=subject, queue=queue, callback=callback or passthru_func}
	return sid
end

function cm.unsubscribe(client, sid)
	request_write(client, {'UNSUB ', sid, '\r\n'})
	client.subscriptions[sid] = nil
end

function cm.auth(client, user, pass) -- crap
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return {user=client.config.user,pass=client.config.pass}
end

function cm.server_info(client) return client.server_information end

function cm.shutdown(client) client.network.socket:shutdown() end

function cm.wait(client, sid, maxcount, timeout)
	local count, socket, msg_t, msg = 1, client.network.socket
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
	
	local toe, loop_run_start = 0, gettime(), loop_run_start -- this toe thing has gone too far... blame you anon
	while true do
		if toe >= 100 then
			local current_time = gettime()
			if timeout_b and timeout_b <= (current_time - loop_run_start) then
				print("timeout on " .. count .. " nats:cm.wait loop " .. (current_time - loop_run_start))
				return
			end
			toe = 0	
		else toe = toe + 1 end
		
		msg = nil
		msg_t = response_read(client)
		if not msg_t then return end
		local op = msg_t.op
		
		if op == 'MSG' or op == 'HMSG' then
			msg_t.client = client
			msg = create_msg(msg_t)
			local st = client.subscriptions[msg.sid]
			if st then msg = st.callback(msg) or msg end
			if sid and sid == msg.sid then break end
			count = count + 1 -- keeping count +1 outof loop for timeout checks information
			if maxcount and count >= maxcount then break end
		elseif op == 'PING' then cm.pong(client)
		elseif op == 'INFO' then
			-- client.information = decode(data.payload) -- notsure if this will overwrite and untested, so lets be safe..
			for k,v in pairs(decode(msg_t.data) or {}) do 
				if k == "no_responders" and v == true then
					print("no_responders caught")
				else
					client.server_information[k] = v
				end
			end
		end
	end
	
	if timeout_b then socket:settimeout(last_timeout_b,'b') end
	if timeout_t then socket:settimeout(last_timeout_t,'t') end

	return msg
end

--
-- nats msg methods
local mm = nats.msg_metatable.methods
function mm.respond(m, data, header, reply)
	if m.reply then m.client:publish(m.reply, data, header, reply) end
end


--
-- client configuration handling - client:option(value)
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

return nats

-- nats.encoded_client_metatable = {
	-- __index = function(self, key)
		-- return rawget(getmetatable(self), "methods")[key] or self.client[key] or rawget(self, key)
	-- end,
	-- __newindex = function(self,key,value)
		-- rawset(self.client, key, value)
	-- end,
	-- methods = {
		-- is_ec = true,
	-- },
-- }

-- function cm.encoded_client(client) -- passthru_func removed for obvious json and http header support.
	-- client.encoding_data = client.encoding_data or encode
	-- client.decoding_data = client.decoding_data or decode
	-- client.encoding_headers = client.encoding_headers or nats.encode_headers
	-- client.decoding_headers = client.decoding_headers or nats.decode_headers
	-- return setmetatable({client = client}, nats.encoded_client_metatable)
-- end

-- local ec = nats.encoded_client_metatable.methods
-- function ec.encode_data(client, data)
	-- if not data then return end
	-- local s, r = pcall(client.encoding_data, data)
	-- if s then return r else print("encode_data error: " .. r .. " : " .. tostring(data)) end
-- end
-- local ec_encode_data = ec.encode_data

-- function ec.decode_data(client, data)
	-- if not data or data == '' then return end
	-- local s, r = pcall(client.decoding_data, data)
	-- if s then return r else print("decode_data error: " .. r .. " : " .. tostring(data)) end
-- end
-- local ec_decode_data = ec.decode_data

-- function ec.encode_headers(client, headers)
	-- if not headers then return end
	-- local s, r = pcall(client.encoding_headers, headers)
	-- if s then return r else print("encode_headers error: " .. r .. " : " .. tostring(headers)) end
-- end
-- local ec_encode_headers = ec.encode_headers

-- function ec.decode_headers(client, headers)
	-- if not headers then return end
	-- local s, r = pcall(client.decoding_headers, headers)
	-- if s then return r else print("decode_headers error: " .. r .. " : "  .. tostring(headers)) end
-- end
-- local ec_decode_headers = ec.decode_headers

-- function ec.publish(client, subject, data, reply, headers)
	-- client = client.client
	-- print("ec publish : " .. subject)
	-- cm.publish(client, subject, ec_encode_data(client, data), reply, ec_encode_headers(client, headers))
-- end

-- function ec.request(client, subject, callback, data, headers)
	-- client = client.client
	-- return cm.request(client, subject, function(data, reply, subject, sid, headers)
		-- data = ec_decode_data(client, data)
		-- headers = ec_decode_headers(client, headers)
		-- local _,r = pcall(callback, data, reply, subject, sid, headers)
		-- return r
	-- end,
	-- ec_encode_data(client, data),
	-- ec_encode_headers(client, headers))
-- end

-- function ec.get(client, subject, data, headers)
	-- client = client.client
	-- local msg_t = cm.get(client, subject, ec_encode_data(client, data), ec_encode_headers(client, headers))
	-- msg_t.data = ec_decode_data(client, msg_t.data)
	-- msg_t.headers = ec_decode_headers(client, msg_t.headers)
	-- return data
-- end

-- function ec.subscribe(client, subject, callback, queue)
	-- client = client.client
	-- cm.subscribe(client, subject, function(data, reply, subject, sid, headers)
		-- data = ec_decode_data(client, data)
		-- headers = ec_decode_headers(client, headers)
		-- local s,r = pcall(callback, data, reply, subject, sid, headers)
		-- if s then return r else print("ec:sub:err " .. r) end
	-- end, queue)
-- end