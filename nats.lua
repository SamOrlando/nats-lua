local nats = {
	_VERSION	 = 'lua-nats-nx 0.0.1',
	_DESCRIPTION = 'Lua client for NATS w/header, tls - https://nats.io',
	_COPYRIGHT   = 'Copyright (C) Sam Orlando 2023 - MIT Cheese Please',
	--
	verbose = false, -- NOT RELATED TO CLIENT VERBOSE OPTION, local output and debugs
	defaults = { -- related to client connection options
		host = '127.0.0.1',
		port = 4222,
		url = 'nats://127.0.0.1:4222',
		timeout = 5,
	},
	network = {}, -- network functions
	request = {}, -- request handling (from client to server)
	response = {}, -- response handing (from server to client)
	nuid = require('nuid'),
}
local network, response, request = nats.network, nats.response, nats.request -- Shortcuts
local verbose = nats.verbose -- (false or nil for off) levels 1-5

local cjson = require('cjson')
local socket = require('socket')
local ssl = require('ssl') -- place with tls_required checks
local type, pcall, rawset, rawget, setmetatable, getmetatable = type, pcall, rawset, rawget, setmetatable, getmetatable
local pairs, ipairs = pairs, ipairs
local floor, tonumber, tostring = math.floor, tonumber, tostring
local encode, decode = cjson.encode, cjson.decode

local insert, concat = table.insert, table.concat
local str_gmatch = string.gmatch
local gettime = socket.gettime

-- TODO Add hooking to new clients so jetstream can influence model creation

-- list of valid configuration options, etc.
local valid_config_toggles = {headers=true,pedantic=true,tls_required=true,echo=true,no_responders=true,verbose=true}
local valid_config_strings = {user=true,pass=true,auth_token=true,name=true,sig=true,jwt=true,nkey=true,protocol=true}

local crlf = '\r\n'
local hdrLine = 'NATS/1.0\r\n'
local max_payload = 1024000

-- common meh functions
local passthru_func = function(...) return unpack({...}) end
nats.passthru_func = passthru_func
local empty_func = function() end
nats.empty_func = empty_func

local function str_explode(s, ss, p)
    local t, i = {}, 1
    for sub in str_gmatch(s, p and ss or "([^" .. ss .. "]+)") do
        t[i] = sub
        i = i + 1
    end
    return t, i-1
end

-- MIMEHeader
-- built in lua native <-> nats header encoding and passthru function
local function encode_header(t)
	local tht = {}
	for k,v in pairs(t or {}) do
		if type(v) == "table" then insert(tht, concat({k, ': ', concat(v, ', '), crlf}))
		else insert(tht, concat({k, ': ', v, crlf})) end
	end
	return concat(tht)
end
nats.encode_header = encode_header

local function decode_header(s)
	if not s then return end
	local tbl = {}
	for line in s:gmatch("(.-)\r\n") do
		local k,v = line:match("(.-):%s*(.*)")
		if k then
			if tbl[k] then tbl[k] = {unpack(tbl[k]), v} -- create array for multiple entries
			else tbl[k] = v end
		end
	end
	return tbl
end
nats.decode_header = decode_header

--
-- nats client/conn object
nats.client_metatable = {
	__index = function(self, key) return rawget(self, key) or rawget(getmetatable(self), "methods")[key] or rawget(getmetatable(self), key) end,
	__newindex = function(self, key, value) print("__newindex client: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
	is_client = true,
	lang = 'LuaJIT',
	version = '0.0.1',
}
local client_metatable = nats.client_metatable

local function create_client(config, n_opts) -- fix default checks
	local client = setmetatable({
		connected = false,
		subscriptions = {},
		server_information = {},
		config = {
			reconnect = nats.defaults.reconnect and nats.defaults.reconnect or false,
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

	return client
end

function nats.is_client(c)
	if rawget(getmetatable(c), "is_client") then return c end
end
local is_client = nats.is_client

--
-- nats encoded client/connection support
nats.encoded_client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or self.client[key] or rawget(getmetatable(self), key) end,
	__newindex = function(_, key, value) print("__newindex encoded client: " .. key .. " with a value of " .. value) end,
	methods = {},
	is_client = true,
	is_encoded = true,
}
local encoded_client_metatable = nats.encoded_client_metatable

function nats.is_encoded(c)
	if rawget(getmetatable(c), "is_encoded") then return c end
end
local is_encoded = nats.is_encoded

-- TODO Go through code an clean your inputs!
-- general check for nats client
function nats.valid_subject(s)
    if type(s) ~= 'string' or s == '' then return nil end
    if s:sub(1, 1) == "." or s:sub(-1) == "." or not s:match('^[^ >]*[>]?$') then return nil end
    return s
end
local valid_subject = nats.valid_subject

--
-- nats msg object
nats.msg_metatable = {
	__index = function(self, key) return rawget(self, key) or rawget(getmetatable(self), "methods")[key] or rawget(getmetatable(self), key) end,
	__newindex = function(self, key, value) print("__newindex msg: " .. key .. " with a value of " .. (value or "nil")) end,
	--__tostring = function(self) return concat({self.subject, ' ', self.reply, ' ', self.sid}) end, -- does not plan for encoding!
	methods = {},
	is_msg = true,
}
local msg_metatable = nats.msg_metatable

function nats.msg(subject, data, reply, header, client, sid)
	return setmetatable({
		client = client,
		data = data,
		header = header,
		subject = subject,
		reply = reply,
		sid = sid,
	}, msg_metatable)
end
local create_msg = nats.msg

function nats.is_msg(m)
	if rawget(getmetatable(m), "is_msg") then return m end
end
local is_msg = nats.is_msg

--
-- network methods
function network.write(client, buffer)
	if #buffer > max_payload then
		client.error("buffer write error: payload too large at " .. #buffer .. " : max_payload is " .. client.server_information.max_payload)
	end
	if verbose then print('->> '..buffer:sub(1,-3)) end
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
		if verbose then print('<<- '..line) end
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
	INFO = function(s) return {op=s[1],data=s[2]} end,
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

		local dheader = {}
		if tonumber(msg_t.header_size) > 0 and network_read(client) == "NATS/1.0" then -- fix for var ref?
		  local hread
		  repeat
			hread = network_read(client)
			insert(dheader, hread)
			insert(dheader, crlf)
		  until #hread == 0
		end
		msg_t.header = concat(dheader)

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

	local slices = {}
	for slice in payload:gmatch('[^%s]+') do insert(slices, slice) end
	if slices[1] and response_methods[slices[1]] then return response_methods[slices[1]](slices, client) end
	return {op = "-ERR", data = payload}
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

function nats.verbose(b)
	if b ~= nil then
		nats.verbose = b and true or false
		verbose = nats.verbose -- local var reassign
	else return verbose end
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

--
-- nats client methods
local cm = nats.client_metatable.methods
local client_info_response = {"verbose","pedantic","jwt","nkey","sig","user","pass","auth_token","tls_required","name","protocol","echo","headers","no_responders"}

-- create into options but dedicated to all clients at moment which does not work long term.
local inbox_prefix = "_INBOX"
local inbox_nuid = nats.nuid({preLen=10,seqLen=0})() -- calling var! we only do ONCE to make a client static inbox --FUTURE: Option for old and new style
local inbox_lid_nuid = nats.nuid({preLen=8,seqLen=2}) -- want some change but not too often
local inbox_table = {}
local sub_nuid = nats.nuid({preLen=10,seqLen=0})

local function msg_stringtable(subject, data, reply, header)
	local st -- send_table
	if data == nil then data = "" end

	if header and #header > 0 then
		st = {'HPUB ', subject}
	else
		st = {'PUB ', subject}
		header = nil
	end

	-- if reply insert
	if reply then
		insert(st, ' ')
		insert(st, reply)
	end

	-- header add size and to total size
	if header then
		header = concat({hdrLine, header, crlf})
		insert(st, ' ')
		insert(st, #header)
		insert(st, ' ')
		insert(st, (#header + #data))
		insert(st, crlf)
		-- insert the header
		insert(st, header)
	else
		-- insert data size
		insert(st, ' ')
		insert(st, #data)
		insert(st, crlf)
	end

	-- insert data
	insert(st, data)
	insert(st, crlf)
	
	return st
end

local function inbox_req_cb(m) -- inbox request callback and handling for wait function
	local lid = str_explode(m.subject or "", '.')[3] -- letter id
	local ibt = lid and inbox_table[lid] -- inbox table

	if ibt then
		if ibt.expire and ibt.expire < gettime() then
			inbox_table[lid] = nil
			return
		end -- TODO: Remove unanswered inbox entries over time (poll/inbox abs expire timer?)

		rawset(m, "sid", inbox_nuid)
		rawset(m, "lid", lid)
		local s, rm = pcall(ibt.callback or passthru_func, m)

		if ibt.count then
			ibt.run = (ibt.run or 0) + 1
			if ibt.run >= ibt.count then
				inbox_table[lid] = nil
				return
			end
		elseif ibt.expire then
		else inbox_table[lid] = nil end
	
		if s then return rm end
	end
end

function cm.connect(client)
	if client.connected and client.network.socket then return true end --review - how do we handle reconnect? do we need to redo all?

	client.network.socket = client.network.socket or create_connection(client.network.opts)
	if not client.network.socket then client.error("Unable to connect socket.") end

	local ct = {
		lang = nats.client_metatable.lang,
		version = nats.client_metatable.version,
	}

	local cfg = rawget(client, "config")
	for _,v in ipairs(client_info_response) do ct[v] = cfg[v] end

	request_write(client, {'CONNECT ', encode(ct), crlf})

	-- gather the server information
	local msg_t = response_read(client)
	if msg_t.op == 'INFO' then
		client.server_information = decode(msg_t.data)
		max_payload = client.server_information.max_payload -- set nats local max_payload to limit buffersize in network write check
	end
	client.connected = cm.ping(client)
	client.inbox_subscribe(client)
end

function cm.ping(client)
	request_write(client, 'PING\r\n')
	local msg_t = response_read(client)
	if msg_t.op == 'PONG' then return true end
	return false
end

function cm.pong(client) request_write(client, 'PONG\r\n') end

function cm.publish(client, subject, data, reply, header)
	request_write(client, msg_stringtable(subject, data, reply, header))
	return create_msg(subject, data, reply, header, client)
end

function cm.subscribe(client, subject, callback, queue)
	local sid = sub_nuid()

	local st = {'SUB ', subject, ' '}
	if queue and queue ~= "" then
		insert(st, queue)
		insert(st, ' ')
	end
	insert(st, sid)
	insert(st, crlf)

	request_write(client, st)
	client.subscriptions[sid] = {subject=subject, queue=queue, callback=callback or passthru_func}
	return sid
end

function cm.unsubscribe(client, sid)
	request_write(client, {'UNSUB ', sid, crlf})
	client.subscriptions[sid] = nil
end

function nats.create_inbox() -- belongs to client and not all. dependent on moving opts to client and not env locals
	local lid = inbox_lid_nuid()
	return concat({inbox_prefix, '.', inbox_nuid, '.', lid}), lid
end
local create_inbox = nats.create_inbox -- vroom

function cm.inbox_subscribe(client) -- create and maintain a subscription just for inbox requests
	local subject = inbox_prefix .. '.' .. inbox_nuid .. '.>'
	local st = {'SUB ', subject, ' '}
	insert(st, inbox_nuid)
	insert(st, crlf)
	request_write(client, st)

	client.subscriptions[inbox_nuid] = {subject=subject, callback=inbox_req_cb} -- not used but put into table for reference
	return inbox_nuid
end

-- // Check for no responder status.
-- if err == nil && len(m.Data) == 0 && m.Header.Get(statusHdr) == noResponders {
	-- m, err = nil, ErrNoResponders
-- }
-- return m, err
function cm.request(client, subject, callback, data, header, sid)
	local reply, lid = concat({inbox_prefix, '.', sub_nuid()})

	if sid and client.subscriptions[sid] then
		reply = client.subscriptions[sid].subject
	-- elseif subject:sub(1,1) == "$" then
		-- sid = cm.subscribe(client, reply, function(m)
			-- cm.unsubscribe(client, m.sid) -- this is nasty and should not be done for consumers/streams... fix
			-- if callback then
				-- local s, rm = pcall(callback, m)
				-- if s then return rm end
			-- end
		-- end)
	else
		reply, lid = create_inbox() -- get reply string and letter id for inbox
		inbox_table[lid] = {created=gettime(), callback=callback}
	end
	
	local msg = cm.publish(client, subject, data, reply, header)
	rawset(msg, "sid", sid or inbox_nuid)
	if not sid then rawset(msg, "lid", lid) end
	
	return msg
end

function cm.get(client, subject, data, header, sid)
	local msg = cm.request(client, subject, nil, data, header, sid)
	local r_msg = cm.wait(client, {
		sid=msg.sid,
		lid=msg.lid,
		timeout=client.config.timeout}) -- add timeout config here
	if r_msg and r_msg.lid then inbox_table[msg.lid] = nil end
	return r_msg -- return results or nothing from timeout
end

-- need to add timer to loop and add callback on termination for wait period
-- add closing func or option to call each or at complete
function cm.poll(client, subject, callback, data, header, wait, count)
	local reply, lid = create_inbox()
	inbox_table[lid] = {created=gettime(), callback=callback, wait=wait or 2, expire=gettime() + (wait or 2), count=count}

	local msg = cm.publish(client, subject, data, reply, header)
	rawset(msg, "sid", inbox_nuid)
	rawset(msg, "lid", lid)

	return msg
end

-- TODO: cm.collect(client, subject, data, header, wait, count) -- blocking

function cm.auth(client, user, pass) -- crap
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return {user=client.config.user,pass=client.config.pass}
end

function cm.server_info(client) return client.server_information end

function cm.shutdown(client) client.network.socket:shutdown() end

-- consider ripping out the fat... make wait calls less checky 
-- client tracking form sid may be required if we handle both at same time?
function cm.wait(client, opts)
	local nc, ec = client
	if is_encoded(client) then
		ec = client
		nc = client.client
	end -- check if we are encoded client - go raw

	opts = opts or {}

	local sid, lid, maxcount, timeout = opts.sid, opts.lid, opts.maxcount, opts.timeout
	local count, socket, msg_t, msg = 1, nc.network.socket
	local timeout_b, timeout_t, last_timeout_b, last_timeout_t
	
	--ugly
	if timeout then
		local ttyp = type(timeout)
		if ttyp == "number" then timeout_b = timeout
		elseif ttyp == "table" then
			timeout_b = timeout[1]
			timeout_t = timeout[2]
		elseif ttyp == "string" then timeout_b = tonumber(timeout) or -1 end
	else
		timeout_b = -1
	end

	last_timeout_b, last_timeout_t = socket:gettimeout()
	if timeout_b then socket:settimeout(timeout_b,'b') end
	if timeout_t then socket:settimeout(timeout_t,'t') end

	local toe, loop_run_start, op = 0, gettime() -- this toe thing has gone too far... blame you looneytunes
	while true do
		if toe >= 100 then
			local current_time = gettime()
			if timeout_b and timeout_b > -1 and timeout_b <= (current_time - loop_run_start) then
				print("timeout on " .. count .. " nats:cm.wait loop " .. (current_time - loop_run_start))
				return
			end
			toe = 0
		else toe = toe + 1 end

		msg = nil -- i like to rinse my hands every cycle because of return
		msg_t = response_read(nc)
		if not msg_t then return end
		op = msg_t.op

		if op == 'MSG' or op == 'HMSG' then
			msg = create_msg(msg_t.subject, msg_t.data, msg_t.reply, msg_t.header, client, msg_t.sid)
			if msg.sid == inbox_nuid then
				msg = inbox_req_cb(msg) or msg
				if lid and msg.lid == lid then break end
			else
				local st = nc.subscriptions[msg.sid]
				if st then
					local s,r = pcall(st.callback or passthru_func, msg)
					msg = s and r or msg
				end
				if sid and sid == msg.sid then break end
			end
			count = count + 1 -- keeping count +1 outof loop for timeout checks information
			if maxcount and count >= maxcount then break end
		elseif op == 'PING' then cm.pong(nc)
		elseif op == 'INFO' then
			-- client.information = decode(data.payload) -- notsure if this will overwrite and untested, so lets be safe..
			for k,v in pairs(decode(msg_t.data) or {}) do
				if k == "no_responders" and v == true then
					print("no_responders caught")
				else
					nc.server_information[k] = v
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
-- TODO equal Compares two msgs, ignores sub but checks all other public fields. 
local mm = nats.msg_metatable.methods
function cm.msg(client, subject, data, reply, header, sid)
	return create_msg(subject, data, reply, header, client, sid)
end

function mm.new(m, subject, data, reply, header, sid)
	return create_msg(subject, data, reply, header, m.client, sid)
end

function mm.respond(m, data, reply, header)
	if m.reply then return m.client:publish(m.reply, data, reply, header) end
end

function mm.publish(m, subject, data, reply, header)
	return m.client and m.client:publish(
		subject or m.subject,
		data or m.data,
		reply or m.reply,
		header or m.header)
end

function mm.request(m, subject, callback, data, header, sid)
	return m.client and m.client:request(
		subject or m.subject,
		callback,
		data or m.data,
		header or m.header,
		sid or m.sid)
end

function mm.get(m, subject, data, header, sid)
	return m.client and m.client:get(
		subject or m.subject,
		data or m.data,
		header or m.header,
		sid or m.sid)
end

function mm.size(m) -- yuk (This is not what I thought it was?)
	local client, ss, rs, hs, ds = m.client, #m.subject, #(m.reply or ""), 0, 0
	local enc_data, enc_header = client and client.encode_data_func, client and client.encode_header_func
	
	if enc_data then ds = #(enc_data(client, m.data) or "")
	else ds = #tostring(m.data or "") end
	if enc_header then hs = #(enc_header(client, m.header) or "")
	else hs = #tostring(m.header or "") end
	return ss + rs + hs + ds
end

-- I hate everything about
-- encoded connection / client
function cm.encoded_client(client, encode_data_func, decode_data_func, encode_header_func, decode_header_func )
	return setmetatable({
		client = client,
		encode_data_func = encode_data_func or encode,
		decode_data_func = decode_data_func or decode,
		encode_header_func = encode_header_func or nats.encode_header,
		decode_header_func = decode_header_func or nats.decode_header,
	}, encoded_client_metatable)
end

local ec = encoded_client_metatable.methods
function ec.encode_data(client, data)
	if not data then return nil end
	local s, r = pcall(client.encode_data_func, data)
	if s then return r else print("encode_data error: " .. r .. " : " .. tostring(data)) end
end
local ec_encode_data = ec.encode_data

function ec.decode_data(client, data)
	if not data or data == '' then return nil end
	local s, r = pcall(client.decode_data_func, data)
	if s then return r else print("decode_data error: " .. r .. " : " .. tostring(data)) end
end
local ec_decode_data = ec.decode_data

function ec.encode_header(client, header)
	if not header then return nil end
	local s, r = pcall(client.encode_header_func, header)
	if s then return r else print("encode_header error: " .. r .. " : " .. tostring(header)) end
end
local ec_encode_header = ec.encode_header

function ec.decode_header(client, header)
	if not header then return nil end
	local s, r = pcall(client.decode_header_func, header)
	if s then return r else print("decode_header error: " .. r .. " : "  .. tostring(header)) end
end
local ec_decode_header = ec.decode_header

function ec.publish(client, subject, data, reply, header)
	return cm.publish(client.client, subject, ec_encode_data(client, data), reply, ec_encode_header(client, header))
end

function ec.request(client, subject, callback, data, header, sid)
	return cm.request(client.client, subject,
		function(m)
			if m.data then rawset(m, "data", ec_decode_data(client, m.data)) end
			if m.header then rawset(m, "header", ec_decode_header(client, m.header)) end
			local s,r = pcall(callback or passthru_func, m)
			return s and r
		end,
		ec_encode_data(client, data),
		ec_encode_header(client, header), sid)
end

function ec.get(client, subject, data, header, sid)
	local msg = cm.get(client.client, subject, ec_encode_data(client, data), ec_encode_header(client, header), sid)
	if msg then
		if msg.data then rawset(msg, "data", ec_decode_data(client, msg.data)) end
		if msg.header then rawset(msg, "header", ec_decode_header(client, msg.header)) end
	end
	return msg
end

function ec.subscribe(client, subject, callback, queue)
	return cm.subscribe(client.client, subject, function(m)
		rawset(m, "client", client) -- takes client from ec.sub call .. meh
		if m.data then rawset(m, "data", ec_decode_data(client, m.data) or nil) end
		if m.header then rawset(m, "header", ec_decode_header(client, m.header) or nil) end
		local s,r = pcall(callback, m)
		if s then return r else print("ec:sub:err " .. r) end
	end, queue)
end

function ec.poll(client, subject, callback, data, header, wait, count)
	return cm.poll(client.client, subject,
		function(m)
			if m.data then rawset(m, "data", ec_decode_data(client, m.data) or nil) end
			if m.header then rawset(m, "header", ec_decode_header(client, m.header) or nil) end
			local s,r = pcall(callback, m)
			if s then return r else print("ec:sub:err " .. r) end
		end,
		ec_encode_data(client, data),
		ec_encode_header(client, header), wait, count)
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
for k,_ in pairs(valid_config_toggles) do
	cm[k] = cm[k] or function(client, b) return config_set(client, k, b) end
end
-- assign methods for all config string options
for k,_ in pairs(valid_config_strings) do
	cm[k] = cm[k] or function(client, s) return config_set(client, k, s) end
end

return nats

-- statusHdr          = "Status"
-- descrHdr           = "Description"
-- lastConsumerSeqHdr = "Nats-Last-Consumer"
-- lastStreamSeqHdr   = "Nats-Last-Stream"
-- consumerStalledHdr = "Nats-Consumer-Stalled"
-- noResponders       = "503"
-- noMessagesSts      = "404"
-- reqTimeoutSts      = "408"
-- jetStream409Sts    = "409"
-- controlMsg         = "100"
-- statusLen          = 3 // e.g. 20x, 40x, 50x
	
-- // badSubject will do quick test on whether a subject is acceptable.
-- // Spaces are not allowed and all tokens should be > 0 in len.
-- func badSubject(subj string) bool {
	-- if strings.ContainsAny(subj, " \t\r\n") {
		-- return true
	-- }
	-- tokens := strings.Split(subj, ".")
	-- for _, t := range tokens {
		-- if len(t) == 0 {
			-- return true
		-- }
	-- }
	-- return false
-- }

-- // badQueue will check a queue name for whitespace.
-- func badQueue(qname string) bool {
	-- return strings.ContainsAny(qname, " \t\r\n")
-- }
