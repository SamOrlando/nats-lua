-- nats.lua - Lua client for NATS (https://nats.io)
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Core client: connection handling, protocol parser, pub/sub, request/reply,
-- headers, keepalive, reconnect and drain. The default runtime is blocking
-- LuaSocket. One coroutine owns the socket. Messages are dispatched to
-- callbacks while that owner is inside client:step(), client:wait(), or a
-- request. client:start() runs the owner only when runtime.spawn is set.
--
-- Everything that looks like background work (reconnect attempts, publish
-- buffering while reconnecting, keepalive PINGs, JetStream pull re-requests,
-- idle heartbeat monitors, flow control replies, KV/object watchers, service
-- endpoints) runs on the owner, from client:step()/wait()/flush()/request()
-- or from the optional background loop. Timers registered with
-- client:add_ticker() fire from that same loop.

local nuid = require('nuid')

local nats = {
	_VERSION     = 'nats-lua 0.4.6',
	_DESCRIPTION = 'Lua client for NATS w/ headers, tls, request/reply, JetStream - https://nats.io',
	_COPYRIGHT   = 'Copyright (C) 2023-2026 Sam Orlando - MIT',
	--
	verbose = false, -- NOT RELATED TO CLIENT VERBOSE OPTION, local output and debugs
	defaults = { -- related to client connection options
		host = '127.0.0.1',
		port = 4222,
		url = 'nats://127.0.0.1:4222',
		timeout = 5,
	},
	nuid = nuid,
}

local type, pcall, rawset, rawget, setmetatable, getmetatable = type, pcall, rawset, rawget, setmetatable, getmetatable
local pairs, ipairs, next, select, error, tostring, tonumber = pairs, ipairs, next, select, error, tostring, tonumber
local floor, random, min = math.floor, math.random, math.min
local os_date, os_time = os.date, os.time
local concat = table.concat
local find, match, gmatch, sub, byte, gsub, format, char =
	string.find, string.match, string.gmatch, string.sub, string.byte, string.gsub, string.format, string.char
local unpack = table.unpack or unpack
local remove = table.remove

-- LuaSocket is loaded on first use by the default runtime, so require('nats')
-- succeeds in a host that supplies its own runtime.
local socket_mod
local function luasocket()
	if socket_mod == nil then
		local ok, mod = pcall(require, 'socket')
		socket_mod = ok and mod or false
	end
	if not socket_mod then error('luasocket is not installed', 2) end
	return socket_mod
end

local function default_read_file(path)
	local f, err = io.open(path, 'rb')
	if not f then return nil, err end
	local data = f:read('*a')
	f:close()
	return data
end

nats.runtime = {
	now = function() return luasocket().gettime() end,
	sleep = function(seconds)
		if seconds and seconds > 0 then luasocket().sleep(seconds) end
	end,
	-- false is the main-thread token where coroutine.running() is nil (Lua 5.1).
	current = function()
		local co = coroutine.running()
		if co == nil then return false end
		return co
	end,
	read_file = default_read_file,
}

function nats.now(client)
	if type(client) == 'table' then
		local c = rawget(client, 'client') or client
		local rt = rawget(c, 'runtime')
		if type(rt) == 'table' and type(rt.now) == 'function' then return rt.now() end
	end
	return nats.runtime.now()
end

function nats.sleep(client, seconds)
	if not seconds or seconds <= 0 then return end
	local rt
	if type(client) == 'table' then
		local c = rawget(client, 'client') or client
		rt = rawget(c, 'runtime')
	end
	if type(rt) ~= 'table' or type(rt.sleep) ~= 'function' then rt = nats.runtime end
	return rt.sleep(seconds)
end

local function now_of(client)
	local rt = rawget(client, 'runtime') or nats.runtime
	return rt.now()
end

local function sleep_of(client, seconds)
	if not seconds or seconds <= 0 then return end
	local rt = rawget(client, 'runtime') or nats.runtime
	return rt.sleep(seconds)
end

local layers = { jetstream = 'nats-jetstream', js = 'nats-jetstream', kvs = 'nats-kvs', objs = 'nats-objs', micro = 'nats-micro' }
function nats.use(...)
	for i = 1, select('#', ...) do
		  local name = select(i, ...)
		  local mod = layers[name]
		  if not mod then error("nats.use: unknown layer '" .. tostring(name) .. "'", 2) end
		  require(mod)
	end
	return nats
end

local CRLF = '\r\n'
local HDR_LINE = 'NATS/1.0\r\n'
local DEFAULT_MAX_PAYLOAD = 1048576

-- ---------------------------------------------------------------------------
-- JSON: lua-cjson when available, otherwise a small pure-Lua codec.
-- ---------------------------------------------------------------------------
local json = {}
do
	local ok, cjson = pcall(require, 'cjson')
	if ok and type(cjson) == 'table' then
		json.encode, json.decode, json.null, json.backend = cjson.encode, cjson.decode, cjson.null, 'cjson'
	else
		local null = setmetatable({}, { __tostring = function() return 'null' end })
		local esc = { ['"'] = '\\"', ['\\'] = '\\\\', ['\b'] = '\\b', ['\f'] = '\\f',
			['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t' }
		local function esc_char(c) return esc[c] or format('\\u%04x', byte(c)) end
		local enc
		local function enc_table(t, out)
			local n, max = 0, 0
			for k in pairs(t) do
				if type(k) ~= 'number' or k <= 0 or k % 1 ~= 0 then n = -1 break end
				n = n + 1
				if k > max then max = k end
			end
			if n > 0 and max == n then
				out[#out + 1] = '['
				for i = 1, n do
					if i > 1 then out[#out + 1] = ',' end
					enc(t[i], out)
				end
				out[#out + 1] = ']'
			else
				out[#out + 1] = '{'
				local first = true
				for k, v in pairs(t) do
					if not first then out[#out + 1] = ',' end
					first = false
					out[#out + 1] = '"' .. gsub(tostring(k), '[%c"\\]', esc_char) .. '":'
					enc(v, out)
				end
				out[#out + 1] = '}'
			end
		end
		enc = function(v, out)
			local tv = type(v)
			if v == nil or v == null then out[#out + 1] = 'null'
			elseif tv == 'boolean' then out[#out + 1] = v and 'true' or 'false'
			elseif tv == 'number' then
				if v ~= v or v == math.huge or v == -math.huge then error('json: cannot encode ' .. tostring(v)) end
				if v % 1 == 0 and v > -2^53 and v < 2^53 then out[#out + 1] = format('%.0f', v)
				else out[#out + 1] = format('%.17g', v) end
			elseif tv == 'string' then out[#out + 1] = '"' .. gsub(v, '[%c"\\]', esc_char) .. '"'
			elseif tv == 'table' then enc_table(v, out)
			else error('json: cannot encode type ' .. tv) end
		end
		function json.encode(v)
			local out = {}
			enc(v, out)
			return concat(out)
		end

		local function utf8_char(cp)
			if cp < 0x80 then return char(cp)
			elseif cp < 0x800 then return char(0xC0 + floor(cp / 0x40), 0x80 + cp % 0x40)
			elseif cp < 0x10000 then
				return char(0xE0 + floor(cp / 0x1000), 0x80 + floor(cp / 0x40) % 0x40, 0x80 + cp % 0x40)
			end
			return char(0xF0 + floor(cp / 0x40000), 0x80 + floor(cp / 0x1000) % 0x40,
				0x80 + floor(cp / 0x40) % 0x40, 0x80 + cp % 0x40)
		end
		local unesc = { b = '\b', f = '\f', n = '\n', r = '\r', t = '\t', ['"'] = '"', ['\\'] = '\\', ['/'] = '/' }
		local dec
		local function skip(s, i) return (find(s, '[^ \t\r\n]', i)) or #s + 1 end
		local function dec_string(s, i) -- i points after the opening quote
			local out, j = {}, i
			while true do
				local k = find(s, '["\\]', j)
				if not k then error('json: unterminated string') end
				out[#out + 1] = sub(s, j, k - 1)
				if byte(s, k) == 34 then return concat(out), k + 1 end
				local c = sub(s, k + 1, k + 1)
				if c == 'u' then
					local cp = tonumber(sub(s, k + 2, k + 5), 16)
					if not cp then error('json: bad unicode escape') end
					j = k + 6
					if cp >= 0xD800 and cp <= 0xDBFF and sub(s, j, j + 1) == '\\u' then
						local lo = tonumber(sub(s, j + 2, j + 5), 16)
						if lo and lo >= 0xDC00 and lo <= 0xDFFF then
							cp = 0x10000 + (cp - 0xD800) * 0x400 + (lo - 0xDC00)
							j = j + 6
						end
					end
					out[#out + 1] = utf8_char(cp)
				else
					out[#out + 1] = unesc[c] or error('json: bad escape')
					j = k + 2
				end
			end
		end
		dec = function(s, i)
			i = skip(s, i)
			local c = byte(s, i)
			if c == 123 then -- {
				local t = {}
				i = skip(s, i + 1)
				if byte(s, i) == 125 then return t, i + 1 end
				while true do
					if byte(s, i) ~= 34 then error('json: expected key at ' .. i) end
					local k
					k, i = dec_string(s, i + 1)
					i = skip(s, i)
					if byte(s, i) ~= 58 then error('json: expected : at ' .. i) end
					local v
					v, i = dec(s, i + 1)
					t[k] = v
					i = skip(s, i)
					c = byte(s, i)
					if c == 125 then return t, i + 1 end
					if c ~= 44 then error('json: expected , or } at ' .. i) end
					i = skip(s, i + 1)
				end
			elseif c == 91 then -- [
				local t, n = {}, 0
				i = skip(s, i + 1)
				if byte(s, i) == 93 then return t, i + 1 end
				while true do
					local v
					v, i = dec(s, i)
					n = n + 1
					t[n] = v
					i = skip(s, i)
					c = byte(s, i)
					if c == 93 then return t, i + 1 end
					if c ~= 44 then error('json: expected , or ] at ' .. i) end
					i = i + 1
				end
			elseif c == 34 then return dec_string(s, i + 1)
			elseif sub(s, i, i + 3) == 'true' then return true, i + 4
			elseif sub(s, i, i + 4) == 'false' then return false, i + 5
			elseif sub(s, i, i + 3) == 'null' then return nil, i + 4
			end
			local num = match(s, '^-?%d+%.?%d*[eE]?[-+]?%d*', i)
			if not num or num == '' then error('json: unexpected character at ' .. i) end
			return tonumber(num), i + #num
		end
		function json.decode(s)
			if type(s) ~= 'string' then error('json: expected string') end
			local v, i = dec(s, 1)
			if skip(s, i) <= #s then error('json: trailing garbage at ' .. i) end
			return v
		end
		json.null, json.backend = null, 'lua'
	end
end
-- integers: on Lua 5.3+ JSON numbers without a fraction (sequences, revisions,
-- counts) are returned as integers (lua-cjson decodes them as floats).
local tointeger = math.tointeger
local function int(v)
	v = tonumber(v)
	if v and tointeger then return tointeger(v) or v end
	return v
end
nats.int = int
if tointeger and json.backend == 'cjson' then
	local cdecode = json.decode
	local function fix(t, depth)
		if depth > 64 then return end
		for k, v in pairs(t) do
			local tv = type(v)
			if tv == 'number' then
				local i = tointeger(v)
				if i and v == v and v > -9007199254740993 and v < 9007199254740993 then t[k] = i end
			elseif tv == 'table' then fix(v, depth + 1) end
		end
	end
	function json.decode(s)
		local v = cdecode(s)
		if type(v) == 'table' then fix(v, 0) elseif type(v) == 'number' then v = tointeger(v) or v end
		return v
	end
end
nats.json = json
local encode, decode = json.encode, json.decode

-- common functions
local passthru_func = function(...) return ... end
nats.timestamp = function(t) return os_date('!%Y-%m-%dT%H:%M:%SZ', t or os_time()) end -- RFC 3339

-- ---------------------------------------------------------------------------
-- Subject validation (ADR-6 naming rules)
-- ---------------------------------------------------------------------------
-- publish / reply subjects: non-empty tokens, no whitespace/control characters
local function bad_subject(s)
	if type(s) ~= 'string' or s == '' then return true end
	if find(s, '[%s%c]') then return true end
	if byte(s, 1) == 46 or byte(s, -1) == 46 or find(s, '..', 1, true) then return true end
	return false
end

-- subscribe subjects: also wildcard placement ('*' and '>' must be whole tokens, '>' last)
local function bad_sub_subject(s)
	if bad_subject(s) then return true end
	if find(s, '[*>]') then
		for tok in gmatch(s, '[^.]+') do
			if #tok > 1 and find(tok, '[*>]') then return true end
		end
		local gt = find(s, '>', 1, true)
		if gt and gt ~= #s then return true end
	end
	return false
end

-- publish subjects repeat a lot: remember the ones already validated
local vcache, vcount = {}, 0
local function bad_pub_subject_cached(s)
	if vcache[s] then return false end
	if bad_subject(s) then return true end
	if vcount >= 4096 then vcache, vcount = {}, 0 end
	vcache[s] = true
	vcount = vcount + 1
	return false
end

local function bad_queue(q) return type(q) ~= 'string' or q == '' or find(q, '[%s%c]') ~= nil end

function nats.valid_subject(s) if bad_sub_subject(s) then return nil end return s end
function nats.valid_publish_subject(s) if bad_subject(s) or find(s, '[*>]') then return nil end return s end
function nats.valid_queue(q) if bad_queue(q) then return nil end return q end

-- ---------------------------------------------------------------------------
-- Headers (ADR-4)
-- ---------------------------------------------------------------------------
-- table -> "Key: value\r\n..." ; array values are emitted one line per value
local function encode_header(t)
	if type(t) == 'string' then return t end
	local out, n = {}, 0
	for k, v in pairs(t or {}) do
		if type(v) == 'table' then
			for _, vv in ipairs(v) do
				n = n + 1
				out[n] = k .. ': ' .. tostring(vv) .. CRLF
			end
		else
			n = n + 1
			out[n] = k .. ': ' .. tostring(v) .. CRLF
		end
	end
	return concat(out)
end
nats.encode_header = encode_header

-- "Key: value\r\n..." (optionally starting with the NATS/1.0 version/status line)
-- -> table; repeated keys become arrays, a status line yields Status/Description
local function decode_header(s)
	if type(s) ~= 'string' then return s end
	local tbl = {}
	for line in gmatch(s, '([^\r\n]+)') do
		local k, v = match(line, '^([^:]+):%s*(.-)%s*$')
		if k then
			local cur = tbl[k]
			if cur == nil then tbl[k] = v
			elseif type(cur) == 'table' then cur[#cur + 1] = v
			else tbl[k] = { cur, v } end
		else
			local code, desc = match(line, '^NATS/%d+%.%d+%s+(%d%d%d)%s*(.-)%s*$')
			if code then
				tbl.Status = code
				if desc ~= '' then tbl.Description = desc end
			end
		end
	end
	return tbl
end
nats.decode_header = decode_header

-- ---------------------------------------------------------------------------
-- Objects / metatables
-- ---------------------------------------------------------------------------
nats.client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] end,
	methods = {},
	lang = 'lua',
	version = '0.4.6',
}
local client_metatable = nats.client_metatable

-- nats encoded client/connection support
nats.encoded_client_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or self.client[key] end,
	__newindex = function(self, key, value) self.client[key] = value end,
	methods = {},
}
local encoded_client_metatable = nats.encoded_client_metatable

nats.msg_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(getmetatable(self), key) end,
	methods = {},
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

local function unwrap(c) return rawget(c, 'client') or c end

local valid_config = {
	-- CONNECT options
	headers = true, pedantic = true, tls_required = true, echo = true, no_responders = true, verbose = true,
	user = true, pass = true, auth_token = true, name = true, sig = true, jwt = true, nkey = true, protocol = true,
	-- client options
	host = true, port = true, url = true, servers = true, unix = true, path = true,
	timeout = true, connect_timeout = true, tcp_nodelay = true,
	reconnect = true, max_reconnect = true, reconnect_wait = true, reconnect_wait_max = true,
	reconnect_jitter = true, reconnect_jitter_tls = true, reconnect_delay_cb = true,
	no_randomize = true, ignore_discovered_servers = true, retry_on_failed_connect = true,
	ping_interval = true, max_pings_out = true, validate_subjects = true, drain_timeout = true,
	tls = true, tls_first = true, creds = true, signature_cb = true, inbox_prefix = true,
	error_cb = true, disconnected_cb = true, reconnected_cb = true, closed_cb = true, lame_duck_cb = true,
	max_chan_len = true, reconnect_buf_size = true, discovered_servers_cb = true, nkey_seed = true,
	nkey_file = true, resolve_all = true, ws_mask = true, ws_headers = true, runtime = true,
}
nats.valid_config = valid_config

local nats_inbox_prefix = "_INBOX"
local nats_inbox_mbid_nuid = nuid({ preLen = 12, seqLen = 10 })
local nats_inbox_lid_nuid = nuid({ preLen = 0, seqLen = 8 }) -- letter ids are an 8-character sequence

local function create_client(config, n_opts)
	local d = nats.defaults
	local function dflt(k, v) if d[k] ~= nil then return d[k] end return v end
	local cfg = {
		reconnect = dflt('reconnect', false),
		max_reconnect = dflt('max_reconnect', 60),
		reconnect_wait = dflt('reconnect_wait', 2),
		reconnect_wait_max = dflt('reconnect_wait_max', 30),
		reconnect_jitter = dflt('reconnect_jitter', 0.1),
		reconnect_jitter_tls = dflt('reconnect_jitter_tls', 1),
		timeout = dflt('timeout', 5),
		connect_timeout = d.connect_timeout,
		ping_interval = dflt('ping_interval', 120),
		max_pings_out = dflt('max_pings_out', 2),
		max_chan_len = dflt('max_chan_len', 64 * 1024),
		tcp_nodelay = dflt('tcp_nodelay', true),
		no_responders = dflt('no_responders', true),
		verbose = dflt('verbose', false),
		protocol = dflt('protocol', 1),
		pedantic = dflt('pedantic', false),
		tls_required = dflt('tls_required', false),
		headers = dflt('headers', true),
		echo = dflt('echo', true),
		validate_subjects = dflt('validate_subjects', true),
		drain_timeout = dflt('drain_timeout', 30),
		user = d.user, pass = d.pass, auth_token = d.auth_token, name = d.name,
		sig = d.sig, jwt = d.jwt, nkey = d.nkey, creds = d.creds, signature_cb = d.signature_cb,
		tls = d.tls, tls_first = d.tls_first, servers = d.servers, url = d.url_option,
		no_randomize = d.no_randomize, ignore_discovered_servers = d.ignore_discovered_servers,
		inbox_prefix = d.inbox_prefix,
		error_cb = d.error_cb, disconnected_cb = d.disconnected_cb, reconnected_cb = d.reconnected_cb,
		closed_cb = d.closed_cb, lame_duck_cb = d.lame_duck_cb, reconnect_delay_cb = d.reconnect_delay_cb,
		reconnect_buf_size = dflt('reconnect_buf_size', 8 * 1024 * 1024),
		discovered_servers_cb = d.discovered_servers_cb, resolve_all = dflt('resolve_all', true),
	}
	for k, v in pairs(config or {}) do cfg[k] = v end

	local client = setmetatable({
		connected = false,
		closed = false,
		socket = false,
		subscriptions = {},
		server_information = { max_payload = DEFAULT_MAX_PAYLOAD }, -- until INFO is received
		config = cfg,
		network = n_opts or {
			host = d.host,
			port = d.port,
			timeout = d.tcp_timeout or -1,
			tls_required = d.tls_required,
		},
		error = nats.error,
		servers = false,       -- server pool (built on first connect)
		current_server = false,
		inbox_prefix = (cfg.inbox_prefix or nats_inbox_prefix),
		inbox_muxed = (cfg.inbox_prefix or nats_inbox_prefix) .. '.' .. nats_inbox_mbid_nuid(),  -- muxed request inbox: <prefix>.<token>
		inbox_table = {},
		inbox_sid = '1', -- reserved; subscribe() starts at "2"
		ssid = 1,
		pings_sent = 0,
		pongs_received = 0,
		last_ping = 0,
		last_sweep = 0,
		lame_duck = false,
		last_error = false,
		reconnecting = false,
		stats = { in_msgs = 0, out_msgs = 0, in_bytes = 0, out_bytes = 0, reconnects = 0, dropped = 0 },
		runtime = type(cfg.runtime) == 'table' and cfg.runtime or nats.runtime,
		_inbuf = false,    -- unparsed protocol bytes
		_ops = false,      -- operations already parsed from _inbuf
		_to = false,
		_rc = false,       -- reconnect state while disconnected
		_tickers = {},
		_rc_hooks = {},   -- internal reconnect listeners (JetStream consumers)
	}, client_metatable)
	return client
end

-- ---------------------------------------------------------------------------
-- Errors
-- ---------------------------------------------------------------------------
function nats.error(message, level) error(message, (level or 1) + 1) end

local function report(client, err)
	rawset(client, 'last_error', err)
	local cb = client.config.error_cb
	if cb then pcall(cb, client, err)
	elseif nats.verbose then print('nats error: ' .. tostring(err)) end
end

-- ---------------------------------------------------------------------------
-- Sockets
-- ---------------------------------------------------------------------------
local function tls_wrap(sock, cfg, host, timeout)
	local ok, ssl = pcall(require, 'ssl')
	if not ok then return nil, 'TLS requested but LuaSec (ssl) is not installed' end
	local params = { mode = 'client', protocol = 'any', options = 'all', verify = 'none' }
	if type(cfg.tls) == 'table' then for k, v in pairs(cfg.tls) do params[k] = v end end
	local s, err = ssl.wrap(sock, params)
	if not s then return nil, 'TLS wrap failed: ' .. tostring(err) end
	if s.sni and host then s:sni(host) end
	s:settimeout(timeout)
	local hs, herr = s:dohandshake()
	if not hs then return nil, 'TLS handshake failed: ' .. tostring(herr) end
	return s
end
nats.tls_wrap = tls_wrap

-- Creates and connects a raw socket for a pool entry. Overridable (tests).
function nats.socket_factory(srv, cfg)
	local timeout = cfg.connect_timeout or cfg.timeout
	if srv.unix then
		local ok, unix = pcall(require, 'socket.unix') -- uses the same LuaSocket the default runtime loads
		if not ok then return nil, 'your build of LuaSocket does not support UNIX domain sockets' end
		local s = (type(unix) == 'table' and unix.stream) and unix.stream() or unix()
		s:settimeout(timeout)
		local cok, err = s:connect(srv.path)
		if not cok then s:close() return nil, 'could not connect to ' .. srv.path .. ' [' .. tostring(err) .. ']' end
		return s
	end
	-- ADR-11: resolve the host name and try every address (randomized unless no_randomize)
	local socket = luasocket()
	local addrs = { srv.host }
	if cfg.resolve_all ~= false and not match(srv.host, '^[%d%.]+$') and not find(srv.host, ':', 1, true) then
		local ok, info = pcall(socket.dns.getaddrinfo, srv.host)
		if ok and type(info) == 'table' and #info > 0 then
			addrs = {}
			local seen = {}
			for _, a in ipairs(info) do
				if a.addr and not seen[a.addr] and (a.family == 'inet' or a.family == 'inet6') then
					seen[a.addr] = true
					addrs[#addrs + 1] = a.addr
				end
			end
			if #addrs == 0 then addrs = { srv.host } end
			if #addrs > 1 and not cfg.no_randomize then
				for i = #addrs, 2, -1 do local j = random(i) addrs[i], addrs[j] = addrs[j], addrs[i] end
			end
		end
	end
	local cerr
	for _, addr in ipairs(addrs) do
		local s, err = (find(addr, ':', 1, true) and socket.tcp6 or socket.tcp)()
		if not s then return nil, err end
		s:settimeout(timeout)
		local cok, e = s:connect(addr, srv.port)
		if cok then
			if cfg.tcp_nodelay ~= nil then s:setoption('tcp-nodelay', cfg.tcp_nodelay and true or false) end
			if srv.ws then return nats.websocket_wrap(s, srv, cfg, timeout) end
			return s
		end
		s:close()
		cerr = e
	end
	return nil, 'could not connect to ' .. srv.host .. ':' .. srv.port .. ' [' .. tostring(cerr) .. ']'
end

-- ---------------------------------------------------------------------------
-- WebSocket transport (ws:// and wss://). Wraps a LuaSocket TCP object into an
-- object with the same send/receive/settimeout/close interface; NATS protocol
-- bytes travel in binary frames. Client frames use a zero mask key unless
-- ws_mask = true (random keys, slower in pure Lua).
-- ---------------------------------------------------------------------------
local WS = {}
WS.__index = WS
local ws_xor
local function ws_mask_payload(data, key)
	if not ws_xor then
		ws_xor = require('nats-sha256')._bxor
	end
	local out, k = {}, { byte(key, 1, 4) }
	for i = 1, #data do out[i] = char(ws_xor(byte(data, i), k[(i - 1) % 4 + 1])) end
	return concat(out)
end
local function ws_frame(op, data, mask)
	local n = #data
	local hdr
	if n < 126 then hdr = char(0x80 + op, 0x80 + n)
	elseif n < 65536 then hdr = char(0x80 + op, 0x80 + 126, floor(n / 256), n % 256)
	else
		local b = {}
		local v = n
		for i = 8, 1, -1 do b[i] = v % 256 v = floor(v / 256) end
		hdr = char(0x80 + op, 0x80 + 127) .. char(unpack(b))
	end
	if mask then
		local key = char(random(0, 255), random(0, 255), random(0, 255), random(0, 255))
		return hdr .. key .. ws_mask_payload(data, key)
	end
	return hdr .. '\0\0\0\0' .. data
end
nats.ws_frame = ws_frame

function WS:settimeout(t) self.timeout = t return self.sock:settimeout(t) end
function WS:setoption(...) if self.sock.setoption then return self.sock:setoption(...) end end
function WS:close() return self.sock:close() end
function WS:shutdown() if self.sock.shutdown then return self.sock:shutdown() end end
function WS:dirty() return #self.buf > 0 or (self.sock.dirty and self.sock:dirty()) end
function WS:getfd() return self.sock.getfd and self.sock:getfd() end
function WS:send(data, i)
	i = i or 1
	if i > 1 then data = sub(data, i) end
	local f = ws_frame(2, data, self.mask)
	local j = 1
	while j <= #f do
		local last, err, partial = self.sock:send(f, j)
		if last then break end
		if err ~= 'timeout' or not partial or partial < j then return nil, err, (i - 1) end
		j = partial + 1
	end
	return #data + i - 1
end
-- decode complete frames from self.raw into self.buf
function WS:decode()
	local raw = self.raw
	while true do
		local n = #raw
		if n < 2 then break end
		local b1, b2 = byte(raw, 1, 2)
		local op, len, pos = b1 % 16, b2 % 128, 3
		if len == 126 then
			if n < 4 then break end
			len = byte(raw, 3) * 256 + byte(raw, 4)
			pos = 5
		elseif len == 127 then
			if n < 10 then break end
			len = 0
			for k = 3, 10 do len = len * 256 + byte(raw, k) end
			pos = 11
		end
		local masked = b2 >= 128
		local key
		if masked then
			if n < pos + 3 then break end
			key = sub(raw, pos, pos + 3)
			pos = pos + 4
		end
		if n < pos + len - 1 then break end
		local payload = sub(raw, pos, pos + len - 1)
		if masked then payload = ws_mask_payload(payload, key) end
		raw = sub(raw, pos + len)
		if op == 0 or op == 1 or op == 2 then self.buf = self.buf .. payload
		elseif op == 8 then self.closed = true
		elseif op == 9 then self.sock:send(ws_frame(10, payload, self.mask))
		end
	end
	self.raw = raw
end
function WS:fill()
	-- block (up to the socket timeout) for one byte, then take whatever else is buffered
	local first, err = self.sock:receive(1)
	if not first then
		if err == 'timeout' or err == 'wantread' then return nil, 'timeout' end
		return nil, err
	end
	self.sock:settimeout(0)
	local data, _, partial = self.sock:receive(65536)
	self.sock:settimeout(self.timeout)
	local got = first .. (data or partial or '')
	if got ~= '' then
		self.raw = self.raw .. got
		self:decode()
		return true
	end
	if err and err ~= 'timeout' and err ~= 'wantread' then return nil, err end
	return nil, 'timeout'
end
-- Payload bytes only. The NATS parser owns line and body framing.
function WS:read_some(max, timeout)
	max = max or 65536
	if timeout ~= nil then self:settimeout(timeout) end
	while #self.buf == 0 do
		if self.closed then return nil, 'closed' end
		local ok, err = self:fill()
		if not ok then return nil, err end
	end
	if #self.buf <= max then
		local data = self.buf
		self.buf = ''
		return data
	end
	local data = sub(self.buf, 1, max)
	self.buf = sub(self.buf, max + 1)
	return data
end
function WS:receive(max)
	return self:read_some(type(max) == 'number' and max or 65536, self.timeout)
end

function nats.websocket_wrap(s, srv, cfg, timeout)
	if srv.tls then
		local ts, terr = tls_wrap(s, cfg, srv.host, timeout)
		if not ts then s:close() return nil, terr end
		s = ts
	end
	local key = {}
	for i = 1, 16 do key[i] = char(random(0, 255)) end
	local b64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
	local k, enc = concat(key), {}
	for i = 1, 16, 3 do
		local a, b, c = byte(k, i, i + 2)
		local v = a * 65536 + (b or 0) * 256 + (c or 0)
		for j = 0, 3 do
			if j <= (c and 3 or b and 2 or 1) then
				local x = floor(v / 2 ^ (18 - 6 * j)) % 64
				enc[#enc + 1] = sub(b64, x + 1, x + 1)
			else enc[#enc + 1] = '=' end
		end
	end
	local req = { 'GET ' .. (srv.ws_path or '/') .. ' HTTP/1.1', 'Host: ' .. srv.host .. ':' .. srv.port,
		'Upgrade: websocket', 'Connection: Upgrade', 'Sec-WebSocket-Key: ' .. concat(enc),
		'Sec-WebSocket-Version: 13' }
	for hk, hv in pairs(cfg.ws_headers or {}) do req[#req + 1] = hk .. ': ' .. hv end
	req[#req + 1] = ''
	req[#req + 1] = ''
	local ok, err = s:send(concat(req, '\r\n'))
	if not ok then s:close() return nil, 'websocket handshake failed: ' .. tostring(err) end
	local status = s:receive('*l')
	if not status or not find(status, '^HTTP/1%.1 101') then
		s:close()
		return nil, 'websocket upgrade refused: ' .. tostring(status)
	end
	repeat
		local line = s:receive('*l')
		if not line then s:close() return nil, 'websocket handshake truncated' end
	until line == ''
	return setmetatable({ sock = s, buf = '', raw = '', mask = cfg.ws_mask and true or false }, WS)
end

-- send everything; returns true or nil, err
local function raw_send(client, data)
	local sock = rawget(client, 'socket')
	if not sock then return nil, 'not connected' end
	local to = client.config.timeout or -1
	if rawget(client, '_to') ~= to then sock:settimeout(to) rawset(client, '_to', to) end
	local i, n = 1, #data
	while true do
		local last, err, partial = sock:send(data, i)
		if last then return true end
		if err ~= 'timeout' or not partial or partial < i then return nil, err end
		i = partial + 1
		if i > n then return true end
	end
end

local READ_MAX = 65536

-- Read between 1 and max bytes. Waits up to timeout seconds for the first byte,
-- then takes whatever else the socket already has. timeout < 0 or nil blocks.
-- A socket may implement read_some(max, timeout) itself (WebSocket wrapper, a
-- host adapter). Otherwise this is LuaSocket: receive(n) waits for n bytes, so
-- the first byte is read alone and the rest is drained with a zero timeout.
local function read_some(sock, timeout, max)
	max = max or READ_MAX
	if max < 1 then max = 1 end
	if type(sock.read_some) == 'function' then return sock:read_some(max, timeout) end
	local to = timeout
	if to == nil or to < 0 then to = -1 end
	sock:settimeout(to)
	local first, err, partial = sock:receive(1)
	if not first then
		if err == 'timeout' or err == 'wantread' then return nil, 'timeout', partial end
		return nil, err or 'closed', partial
	end
	if max == 1 then return first end
	sock:settimeout(0)
	local rest, _, rp = sock:receive(max - 1)
	sock:settimeout(to)
	local extra = rest or rp or ''
	if extra ~= '' then return first .. extra end
	return first
end

local function close_socket(client)
	local sock = rawget(client, 'socket')
	if sock then pcall(sock.close, sock) end
	rawset(client, 'socket', false)
	rawset(client, 'connected', false)
	rawset(client, '_inbuf', false)
	rawset(client, '_ops', false)
	rawset(client, '_to', false)
end

-- ---------------------------------------------------------------------------
-- Protocol parser
-- ---------------------------------------------------------------------------
local OP_PING, OP_PONG, OP_OK = { op = 'PING' }, { op = 'PONG' }, { op = '+OK' }

-- splits a received header block into header lines, status and description
local function split_header(blk)
	local e = find(blk, CRLF, 1, true)
	if not e then return nil end
	local status, desc
	if e > 9 then -- "NATS/1.0 503 No Responders"
		status, desc = match(sub(blk, 1, e - 1), '^NATS/%d+%.%d+%s+(%d%d%d)%s*(.-)%s*$')
		if desc == '' then desc = nil end
	end
	local rest = sub(blk, e + 2, -3) -- drop version line and the terminating blank line
	if rest == '' then rest = nil end
	return rest, status, desc
end
nats.split_header = split_header

local function parse_line(line)
	local c = byte(line, 1)
	if not c then return { op = '-ERR', data = 'unknown protocol operation: ' .. line } end
	if c == 77 then -- MSG <subject> <sid> [reply] <size>
		local subj, sid, a, b = match(line, '^MSG%s+(%S+)%s+(%S+)%s+(%S+)%s*(%S*)')
		if not subj then return { op = '-ERR', data = 'parser error: ' .. line } end
		local op
		if b == '' then op = { op = 'MSG', subject = subj, sid = sid, size = tonumber(a), hsize = 0 }
		else op = { op = 'MSG', subject = subj, sid = sid, reply = a, size = tonumber(b), hsize = 0 } end
		-- size 0 is an empty payload; `not op.size` would treat it as missing
		if op.size == nil or op.size < 0 or (op.hsize or 0) > op.size then
			return { op = '-ERR', data = 'parser error: ' .. line }
		end
		return op, op.size + 2
	elseif c == 72 then -- HMSG <subject> <sid> [reply] <hdr_size> <total_size>
		local subj, sid, a, b, e = match(line, '^HMSG%s+(%S+)%s+(%S+)%s+(%S+)%s+(%S+)%s*(%S*)')
		if not subj then return { op = '-ERR', data = 'parser error: ' .. line } end
		local op
		if e == '' then op = { op = 'HMSG', subject = subj, sid = sid, hsize = tonumber(a), size = tonumber(b) }
		else op = { op = 'HMSG', subject = subj, sid = sid, reply = a, hsize = tonumber(b), size = tonumber(e) } end
		if op.size == nil or op.hsize == nil or op.size < 0 or op.hsize < 0 or op.hsize > op.size then
			return { op = '-ERR', data = 'parser error: ' .. line }
		end
		return op, op.size + 2
	elseif line == 'PING' then return OP_PING
	elseif line == 'PONG' then return OP_PONG
	elseif line == '+OK' then return OP_OK
	elseif c == 73 and sub(line, 1, 5) == 'INFO ' then return { op = 'INFO', data = sub(line, 6) }
	elseif c == 45 and sub(line, 1, 4) == '-ERR' then
		local m = match(line, "^%-ERR%s+'?(.-)'?%s*$") or line
		return { op = '-ERR', data = m }
	end
	return { op = '-ERR', data = 'unknown protocol operation: ' .. line }
end

local function fill_body(op, body)
	local hs = op.hsize or 0
	if hs > 0 then
		op.header, op.status, op.description = split_header(sub(body, 1, hs))
		op.data = sub(body, hs + 1, -3)
	else
		op.data = sub(body, 1, -3)
	end
	return op
end

-- Parse complete protocol operations out of a byte string.
-- Returns an array of ops and the unparsed suffix. PING, PONG and +OK are
-- the shared singleton tables. An empty or partial buffer returns no ops.
function nats.feed(buf)
	if type(buf) ~= 'string' or buf == '' then return {}, buf or '' end
	local ops, i, n = {}, 1, #buf
	while i <= n do
		local e = find(buf, '\n', i, true)
		if not e then break end
		local line = sub(buf, i, e - 1)
		if byte(line, -1) == 13 then line = sub(line, 1, -2) end
		if nats.verbose then print('<<- ' .. line) end
		local op, need = parse_line(line)
		if need then
			if n < e + need then break end
			ops[#ops + 1] = fill_body(op, sub(buf, e + 1, e + need))
			i = e + 1 + need
		else
			ops[#ops + 1] = op
			i = e + 1
		end
	end
	if i > n then return ops, '' end
	return ops, sub(buf, i)
end

local function pop_op(client)
	local q = rawget(client, '_ops')
	if not q or #q == 0 then return nil end
	return remove(q, 1)
end

-- Reads one protocol operation. Returns an op table, or nil, 'timeout' / err.
-- MSG/HMSG ops: {op, subject, sid, reply, data, header, status, description}
local function response_read(client, timeout)
	local queued = pop_op(client)
	if queued then return queued end
	local sock = rawget(client, 'socket')
	if not sock then return nil, 'not connected' end
	local deadline
	if timeout and timeout >= 0 then deadline = now_of(client) + timeout end
	local cap = (client.server_information and client.server_information.max_payload or DEFAULT_MAX_PAYLOAD) + 8192
	while true do
		local left = timeout
		if deadline then
			left = deadline - now_of(client)
			if left < 0 then left = 0 end
		end
		local data, err, partial = read_some(sock, left, READ_MAX)
		-- Reads change the socket timeout. The next write must apply its own.
		rawset(client, '_to', false)
		if (not data or data == '') and partial and partial ~= '' then data = partial end
		if data and data ~= '' then
			local buf = (rawget(client, '_inbuf') or '') .. data
			if #buf > cap and not find(buf, '\n', 1, true) then
				rawset(client, '_inbuf', false)
				return nil, 'protocol buffer exceeded'
			end
			local ops, rest = nats.feed(buf)
			rawset(client, '_inbuf', (rest ~= '' and rest) or false)
			if ops[1] then
				if ops[2] then
					local q = {}
					for i = 2, #ops do q[#q + 1] = ops[i] end
					rawset(client, '_ops', q)
				end
				return ops[1]
			end
			if deadline and now_of(client) >= deadline then return nil, 'timeout' end
		else
			if err == 'timeout' or err == 'wantread' or err == nil then return nil, 'timeout' end
			rawset(client, '_inbuf', false)
			return nil, err or 'closed'
		end
	end
end
nats.response_read = response_read

-- ---------------------------------------------------------------------------
-- Server pool / URLs (ADR-40)
-- ---------------------------------------------------------------------------
local function parse_url(u, default_port)
	if type(u) == 'table' then return u end
	u = tostring(u)
	local scheme, rest = match(u, '^(%a[%w+.-]*)://(.*)$')
	if not scheme then rest = u end
	local userinfo, hostport = match(rest, '^(.*)@([^@]*)$')
	if not userinfo then hostport = rest end
	local path
	hostport, path = match(hostport, '^([^/]*)(.*)$')
	local host, port = match(hostport, '^%[(.-)%]:?(%d*)$')
	if not host then
		host, port = match(hostport, '^([^:]*):?(%d*)$')
	end
	local srv = { url = u, host = (host and host ~= '') and host or '127.0.0.1',
		port = tonumber(port) or default_port or 4222, attempts = 0 }
	if scheme == 'tls' or scheme == 'wss' then srv.tls = true end
	if scheme == 'ws' or scheme == 'wss' then
		srv.ws = true
		srv.ws_path = (path and path ~= '') and path or '/'
		if not tonumber(port) then srv.port = default_port or (scheme == 'wss' and 443 or 80) end
	end
	if userinfo then
		local user, pass = match(userinfo, '^([^:]*):(.*)$')
		if user then srv.user, srv.pass = user, pass else srv.token = userinfo end
	end
	return srv
end
nats.parse_url = parse_url

local function shuffle(t)
	for i = #t, 2, -1 do
		local j = random(i)
		t[i], t[j] = t[j], t[i]
	end
	return t
end

local function build_pool(client)
	local cfg, net = client.config, client.network
	local pool, list = {}, cfg.servers or cfg.url
	if type(list) == 'string' then
		local t = {}
		for u in gmatch(list, '[^,%s]+') do t[#t + 1] = u end
		list = t
	end
	if type(list) == 'table' and #list > 0 then
		for _, u in ipairs(list) do pool[#pool + 1] = parse_url(u) end
		if #pool > 1 and not cfg.no_randomize then shuffle(pool) end
	elseif net.unix or net.path then
		pool[1] = { unix = true, path = net.path, host = net.path, port = 0, attempts = 0 }
	else
		pool[1] = { host = net.host or nats.defaults.host, port = tonumber(net.port) or nats.defaults.port,
			tls = net.tls_required, attempts = 0 }
	end
	rawset(client, 'servers', pool)
	return pool
end

local function merge_discovered(client, urls)
	if client.config.ignore_discovered_servers or type(urls) ~= 'table' then return end
	local pool = client.servers
	local known, added = {}, false
	for _, s in ipairs(pool) do known[s.host .. ':' .. s.port] = true end
	for _, u in ipairs(urls) do
		local s = parse_url(u)
		local key = s.host .. ':' .. s.port
		if not known[key] then
			s.discovered = true
			known[key] = true
			pool[#pool + 1] = s
			added = true
		end
	end
	local cb = client.config.discovered_servers_cb
	if added and cb then pcall(cb, client) end
end

-- ---------------------------------------------------------------------------
-- Connect / reconnect
-- ---------------------------------------------------------------------------
local cm = nats.client_metatable.methods
local client_info_response = { "verbose", "pedantic", "jwt", "nkey", "sig", "user", "pass", "auth_token",
	"tls_required", "name", "protocol", "echo", "headers", "no_responders" }

local function runtime_read(client, path)
	local rt = (client and rawget(client, 'runtime')) or nats.runtime
	local fn = rt.read_file or nats.runtime.read_file
	return fn(path)
end

local function read_creds(creds, client)
	local content = creds
	if not find(creds, '-----BEGIN', 1, true) then
		local data, err = runtime_read(client, creds)
		if not data then return nil, 'cannot read creds file: ' .. tostring(err) end
		content = data
	end
	local jwt = match(content, '%-%-%-%-%-BEGIN NATS USER JWT%-%-%-%-%-%s*([^%s]+)')
	local seed = match(content, '%-%-%-%-%-BEGIN USER NKEY SEED%-%-%-%-%-%s*([^%s]+)')
	if not jwt then return nil, 'creds: user JWT not found' end
	return jwt, seed
end
nats.read_creds = read_creds

local function build_connect(client, srv, info)
	local cfg = client.config
	local ct = { lang = client_metatable.lang, version = client_metatable.version }
	for _, v in ipairs(client_info_response) do ct[v] = cfg[v] end
	ct.headers = cfg.headers ~= false
	ct.no_responders = (cfg.no_responders and ct.headers) and true or false
	if srv.user then ct.user, ct.pass = srv.user, srv.pass end
	if srv.token then ct.auth_token = srv.token end
	if (srv.tls or cfg.tls_required) then ct.tls_required = true end
	local function builtin_sign(seed, nonce)
		local kp = rawget(client, '_kp')
		if not kp or kp._encoded ~= seed then
			local nkeys = require('nats-nkeys')
			local err
			kp, err = nkeys.from_seed(seed)
			if not kp then return nil, 'invalid nkey seed: ' .. tostring(err) end
			rawset(client, '_kp', kp)
		end
		return kp:sign_nonce(nonce), kp
	end
	if cfg.creds then
		local jwt, seed = read_creds(cfg.creds, client)
		if not jwt then return nil, seed end
		ct.jwt = jwt
		if info.nonce then
			if cfg.signature_cb then ct.sig = cfg.signature_cb(info.nonce, seed)
			elseif seed then
				local sig, kerr = builtin_sign(seed, info.nonce)
				if not sig then return nil, kerr end
				ct.sig = sig
			else return nil, 'creds file without nkey seed' end
		end
	else
		local seed = cfg.nkey_seed
		if not seed and cfg.nkey_file then
			local data, ferr = runtime_read(client, cfg.nkey_file)
			if not data then return nil, 'cannot read nkey file: ' .. tostring(ferr) end
			seed = match(data, '(S[A-Z2-7]+)')
		end
		if not seed and type(cfg.nkey) == 'string' and sub(cfg.nkey, 1, 1) == 'S' then seed = cfg.nkey end
		if seed then
			local sig, kp = builtin_sign(seed, info.nonce or '')
			if not sig then return nil, kp end
			if not cfg.jwt then ct.nkey = kp:public_key() end
			if info.nonce then ct.sig = sig end
		elseif (cfg.nkey or cfg.jwt) and info.nonce and not cfg.sig then
			if not cfg.signature_cb then return nil, 'nkey auth requires a signature_cb(nonce) or nkey_seed' end
			ct.sig = cfg.signature_cb(info.nonce)
		end
	end
	return 'CONNECT ' .. encode(ct) .. CRLF
end

local sub_line -- forward

-- single connection attempt to one pool entry: INFO -> (TLS) -> CONNECT/PING -> PONG
local function connect_once(client, srv)
	local cfg = client.config
	local ct = cfg.connect_timeout or cfg.timeout
	close_socket(client)
	local sock, err = nats.socket_factory(srv, cfg)
	if not sock then return nil, err end
	local tls_done = false
	if cfg.tls_first then
		sock, err = tls_wrap(sock, cfg, srv.host, ct)
		if not sock then return nil, err end
		tls_done = true
	end
	rawset(client, 'socket', sock)

	local op, rerr = response_read(client, ct)
	if not op then close_socket(client) return nil, 'no INFO from server: ' .. tostring(rerr) end
	if op.op ~= 'INFO' then
		close_socket(client)
		return nil, 'unexpected greeting: ' .. tostring(op.op) .. (op.data and (' ' .. tostring(op.data)) or '')
	end
	local ok, info = pcall(decode, op.data)
	if not ok or type(info) ~= 'table' then close_socket(client) return nil, 'invalid INFO json' end
	info.max_payload = floor(tonumber(info.max_payload) or DEFAULT_MAX_PAYLOAD)

	if not tls_done and (info.tls_required or cfg.tls_required or srv.tls) then
		local tsock, terr = tls_wrap(sock, cfg, srv.host, ct)
		if not tsock then close_socket(client) return nil, terr end
		rawset(client, 'socket', tsock)
	end

	local connect, cerr = build_connect(client, srv, info)
	if not connect then close_socket(client) return nil, cerr end
	local sok, serr = raw_send(client, connect .. 'PING\r\n')
	if not sok then close_socket(client) return nil, serr end

	local deadline = now_of(client) + ct
	while true do
		local op, e = response_read(client, deadline - now_of(client))
		if not op then close_socket(client) return nil, e == 'timeout' and 'timeout waiting for PONG' or e end
		if op == OP_PONG then break
		elseif op.op == '-ERR' then close_socket(client) return nil, op.data
		elseif op == OP_PING then raw_send(client, 'PONG\r\n')
		end
	end

	rawset(client, 'server_information', info)
	rawset(client, 'current_server', srv)
	rawset(client, 'connected', true)
	rawset(client, 'lame_duck', false)
	rawset(client, 'pings_sent', 0)
	rawset(client, 'pongs_received', 0)
	rawset(client, 'last_ping', now_of(client))
	srv.attempts = 0
	merge_discovered(client, info.connect_urls)

	-- (re)subscribe: inbox mux + all live subscriptions
	local buf = { 'SUB ', client.inbox_muxed, '.* ', client.inbox_sid, CRLF }
	for sid, s in pairs(client.subscriptions) do
		if sid ~= client.inbox_sid then
			buf[#buf + 1] = sub_line(s.subject, s.queue, sid)
			if s.max then
				local left = s.max - (s.delivered or 0)
				if left > 0 then buf[#buf + 1] = 'UNSUB ' .. sid .. ' ' .. left .. CRLF end
			end
		end
	end
	local rok, rerr2 = raw_send(client, concat(buf))
	if not rok then close_socket(client) return nil, rerr2 end
	return true
end

local function backoff(client, attempt, srv)
	local cfg = client.config
	if cfg.reconnect_delay_cb then return cfg.reconnect_delay_cb(attempt) or 0 end
	local base = min((cfg.reconnect_wait or 2) * 2 ^ (attempt - 1), cfg.reconnect_wait_max or 30)
	local jitter = (srv and srv.tls) and cfg.reconnect_jitter_tls or cfg.reconnect_jitter
	return base + random() * (jitter or 0)
end

-- Reconnect state machine (ADR-40). While disconnected, client._rc holds the
-- server order, attempt counter, next attempt time and the publish buffer.
-- Attempts run on the connection owner (step/wait; the publish returns) or
-- block inside client:reconnect(). Publishes are buffered up to reconnect_buf_size bytes and
-- flushed right after the subscriptions are restored.
local function begin_reconnect(client, reason)
	local cfg = client.config
	close_socket(client)
	local rc = rawget(client, '_rc')
	if not rc then
		local pool = client.servers or build_pool(client)
		local cur = client.current_server
		local order = {}
		for _, s in ipairs(pool) do if s ~= cur then order[#order + 1] = s end end
		if not cfg.no_randomize then shuffle(order) end
		if cur then order[#order + 1] = cur end
		if #order == 0 then order[1] = cur end
		rc = { order = order, attempt = 0, next_at = 0, max = cfg.max_reconnect or 60,
			pend = {}, pend_size = 0, last_err = reason }
		rawset(client, '_rc', rc)
		rawset(client, 'reconnecting', true)
		if cfg.disconnected_cb then pcall(cfg.disconnected_cb, client, reason) end
	end
	return rc
end

local function rc_flush(client, rc)
	if #rc.pend == 0 then return true end
	local data = concat(rc.pend)
	local ok = raw_send(client, data)
	if ok then
		client.stats.out_bytes = client.stats.out_bytes + #data
		rc.pend, rc.pend_size = {}, 0
		return true
	end
	rc.pend = { data }
	return false
end

-- I/O failure while a user callback is on the stack is reported and does not
-- raise, so the pcall around the callback does not relabel it as a callback error.
-- The original caller still gets the raise.
local function in_callback(nc)
	return (rawget(nc, '_in_cb') or 0) > 0
end

local function io_error(nc, msg)
	if in_callback(nc) then
		report(nc, msg)
		return false
	end
	nc.error(msg, 3)
end

local function call_user(nc, fn, ...)
	rawset(nc, '_in_cb', (rawget(nc, '_in_cb') or 0) + 1)
	local packed = { pcall(fn, ...) }
	local d = (rawget(nc, '_in_cb') or 1) - 1
	rawset(nc, '_in_cb', d > 0 and d or false)
	return packed[1], packed[2]
end

-- performs the reconnect attempts that are due; block = sleep until the next one.
-- returns true once connected, false if still waiting; raises when giving up
-- (unless a user callback is on the stack).
local function rc_step(client, block)
	local rc = rawget(client, '_rc')
	if not rc then return client.connected end
	local cfg = client.config
	while true do
		local now = now_of(client)
		if now < rc.next_at then
			if not block then return false end
			sleep_of(client, rc.next_at - now)
		end
		local order = rc.order
		local n = #order
		local srv = order[rc.attempt % n + 1]
		rc.attempt = rc.attempt + 1
		srv.attempts = (srv.attempts or 0) + 1
		local ok, err = connect_once(client, srv)
		if ok and rc_flush(client, rc) then
			rawset(client, '_rc', false)
			rawset(client, 'reconnecting', false)
			client.stats.reconnects = client.stats.reconnects + 1
			for _, h in ipairs(client._rc_hooks) do pcall(h, client) end
			if cfg.reconnected_cb then pcall(cfg.reconnected_cb, client) end
			return true
		end
		if ok then close_socket(client) err = 'flush of reconnect buffer failed' end
		rc.last_err = err
		if rc.max >= 0 and rc.attempt >= rc.max * n then
			rawset(client, '_rc', false)
			rawset(client, 'reconnecting', false)
			rawset(client, 'closed', true)
			if cfg.closed_cb then pcall(cfg.closed_cb, client) end
			io_error(client, 'connection error: reconnect failed (' .. tostring(err) .. ')')
			return false
		end
		if rc.attempt >= n then
			rc.next_at = now_of(client) + backoff(client, floor(rc.attempt / n), order[rc.attempt % n + 1])
		end
	end
end

local function buffering(client)
	local b = client.config.reconnect_buf_size
	return b and b > 0
end

local function do_reconnect(client, reason)
	begin_reconnect(client, reason)
	return rc_step(client, true)
end

-- network failure: reconnect if enabled, otherwise raise. Returns true when
-- connected again, false when the owner will retry on a later step/wait.
local function io_failure(client, err)
	close_socket(client)
	if client.closed or not client.config.reconnect then
		io_error(client, 'connection error: ' .. tostring(err))
		return false
	end
	if not rawget(client, '_rc') then report(client, 'disconnected: ' .. tostring(err)) end
	begin_reconnect(client, err)
	return rc_step(client, not buffering(client))
end

-- is_pub: PUB/HPUB data that may be buffered while reconnecting. Control lines
-- (SUB/UNSUB/PING/PONG) are not buffered: subscriptions are restored from the
-- subscription table after a reconnect.
local function network_write(client, buffer, is_pub, retried)
	if not client.connected then
		local rc = rawget(client, '_rc')
		if not rc then
			if client.closed or not client.config.reconnect then
				io_error(client, 'not connected')
				return false
			end
			if not io_failure(client, 'not connected') and not client.connected then return false end
			return network_write(client, buffer, is_pub, true)
		end
		if not is_pub then return end
		if buffering(client) then
			if rc.pend_size + #buffer > client.config.reconnect_buf_size then
				io_error(client, 'reconnect buffer exceeded (' .. client.config.reconnect_buf_size .. ' bytes)')
				return false
			end
			rc.pend[#rc.pend + 1] = buffer
			rc.pend_size = rc.pend_size + #buffer
			return
		end
		rc_step(client, true)
	end
	if nats.verbose then print('->> ' .. sub(buffer, 1, -3)) end
	local ok, err = raw_send(client, buffer)
	if not ok then
		if retried then
			io_error(client, 'connection error: ' .. tostring(err))
			return false
		end
		if not io_failure(client, err) and not client.connected then return false end
		return network_write(client, buffer, is_pub, true)
	end
	client.stats.out_bytes = client.stats.out_bytes + #buffer
end

-- request handling methods (to server)
local function request_write(client, buffer)
	local bt = type(buffer)
	if bt == 'string' then network_write(client, buffer)
	elseif bt == 'table' then network_write(client, concat(buffer))
	else client.error('argument error: ' .. bt) end
end

-- The owner is the coroutine inside connect/step/wait/flush/get. The same
-- coroutine may re-enter (JetStream publish from a callback calls get).
-- A different coroutine raises, unless a background owner is running: then
-- the call is queued and runtime.park waits until that owner runs it.
local function owner_token(nc)
	local rt = rawget(nc, 'runtime') or nats.runtime
	local tok
	if type(rt.current) == 'function' then tok = rt.current() else tok = coroutine.running() end
	if tok == nil then return false end
	return tok
end

local function is_foreign(nc)
	local token = owner_token(nc)
	local loop_owner = rawget(nc, '_loop_owner')
	if loop_owner ~= nil then return loop_owner ~= token end
	local owner = rawget(nc, '_owner')
	if owner == nil then return false end
	return owner ~= token
end

local function enter_owner(nc)
	local token = owner_token(nc)
	local owner = rawget(nc, '_owner')
	if owner == nil then
		rawset(nc, '_owner', token)
		rawset(nc, '_owner_depth', 1)
	elseif owner ~= token then
		nc.error('connection is in use', 2)
	else
		rawset(nc, '_owner_depth', (rawget(nc, '_owner_depth') or 1) + 1)
	end
end

local function leave_owner(nc)
	local d = (rawget(nc, '_owner_depth') or 1) - 1
	if d <= 0 then
		rawset(nc, '_owner', nil)
		rawset(nc, '_owner_depth', nil)
	else
		rawset(nc, '_owner_depth', d)
	end
end

local function with_owner(nc, fn)
	enter_owner(nc)
	local ok, a, b, c = pcall(fn)
	leave_owner(nc)
	if not ok then error(a, 0) end
	return a, b, c
end

local function service_one_cmd(nc)
	if (rawget(nc, '_serving') or 0) > 0 then return false end
	local q = rawget(nc, '_cmds')
	if not q or #q == 0 then return false end
	local cmd = remove(q, 1)
	rawset(nc, '_serving', 1)
	local results = { pcall(cmd.fn) }
	rawset(nc, '_serving', nil)
	cmd.ok = results[1]
	cmd.r1, cmd.r2, cmd.r3 = results[2], results[3], results[4]
	cmd.done = true
	-- Resume a parked caller so it can queue the next command before we read.
	-- Hosts that schedule coroutines themselves can leave runtime.resume unset.
	local resume = nc.runtime.resume
	if type(resume) == 'function' and cmd.co then pcall(resume, cmd.co) end
	return true
end

local function guard(nc, fn)
	nc = unwrap(nc)
	if is_foreign(nc) then
		if rawget(nc, '_loop_owner') ~= nil then
			local q = rawget(nc, '_cmds')
			if not q then q = {} rawset(nc, '_cmds', q) end
			local cmd = { fn = fn, co = coroutine.running() }
			q[#q + 1] = cmd
			local wake = nc.runtime.wakeup
			if type(wake) == 'function' then pcall(wake, nc) end
			local park = nc.runtime.park
			if type(park) ~= 'function' then
				nc.error('runtime.park is required to call the background owner', 2)
			end
			park(cmd)
			if not cmd.ok then error(cmd.r1, 0) end
			return cmd.r1, cmd.r2, cmd.r3
		end
		nc.error('connection is in use', 2)
	end
	return with_owner(nc, fn)
end

--
-- nats module core functions
function nats.client(opts, host, port, timeout)
	local c_opts = {}
	if type(opts) == "string" then
		timeout, port, host, opts = port, host, opts, nil
	end
	-- scrub scrub!
	for k, v in pairs(opts or {}) do
		if valid_config[k] then c_opts[k] = v end
	end
	return create_client(c_opts, {
		host = host or c_opts.host or nats.defaults.host,
		port = port or c_opts.port or nats.defaults.port,
		path = c_opts.path, unix = c_opts.unix,
		timeout = timeout or nats.defaults.tcp_timeout or -1,
		tls_required = c_opts.tls_required or nats.defaults.tls_required,
	})
end

function nats.connect(...)
	local client
	if type((...)) == 'table' then client = nats.client(...)
	else
		local host, port, timeout = ...
		client = nats.client(nil, host, port, tonumber(timeout))
	end
	client:connect()
	return client
end

function cm.connect(client)
	client = unwrap(client)
	return guard(client, function()
		if client.connected and client.socket then return true end
		rawset(client, 'closed', false)
		local pool = client.servers or build_pool(client)
		local last_err
		for _, srv in ipairs(pool) do
			srv.attempts = (srv.attempts or 0) + 1
			local ok, err = connect_once(client, srv)
			if ok then return true end
			last_err = err
		end
		if client.config.retry_on_failed_connect and client.config.reconnect then
			return do_reconnect(client, last_err)
		end
		client.error(last_err or 'could not connect', 2)
	end)
end

-- force a reconnect (ADR-40 on-demand reconnect)
function cm.reconnect(client)
	client = unwrap(client)
	return guard(client, function()
		return do_reconnect(client, 'reconnect requested')
	end)
end

-- replace the server pool used for (re)connects (ADR-40)
function cm.set_server_pool(client, servers)
	client = unwrap(client)
	local cfg = client.config
	cfg.servers = servers
	build_pool(client)
	return client.servers
end

function cm.is_connected(client) return unwrap(client).connected == true end
function cm.is_reconnecting(client) return rawget(unwrap(client), '_rc') and true or false end
-- bytes of publishes buffered while reconnecting
function cm.buffered(client)
	local rc = rawget(unwrap(client), '_rc')
	return rc and rc.pend_size or 0
end
-- ADR-40 connection status: 'connected' | 'reconnecting' | 'closed' | 'disconnected'
function cm.status_string(client)
	local nc = unwrap(client)
	if nc.connected then return 'connected' end
	if rawget(nc, '_rc') then return 'reconnecting' end
	if nc.closed then return 'closed' end
	return 'disconnected'
end

-- timers driven by the owner (step/wait): add_ticker(interval_seconds, fn(client, now)) -> ticker
-- fn returning false removes the ticker.
function cm.add_ticker(client, interval, fn)
	local nc = unwrap(client)
	local t = { interval = interval, fn = fn, due = now_of(nc) + interval }
	local list = nc._tickers
	list[#list + 1] = t
	return t
end
-- internal: fn(client) is called after every successful reconnect
function cm.on_reconnect(client, fn)
	local l = unwrap(client)._rc_hooks
	l[#l + 1] = fn
	return fn
end
function cm.off_reconnect(client, fn)
	local l = unwrap(client)._rc_hooks
	for i = #l, 1, -1 do if l[i] == fn then table.remove(l, i) end end
end
function cm.remove_ticker(client, t)
	local list = unwrap(client)._tickers
	for i = #list, 1, -1 do if list[i] == t then table.remove(list, i) end end
end

-- legacy module level inbox (not bound to a client's muxed subscription)
function nats.new_inbox(inbox_prefix, mid)
	return concat({
		inbox_prefix or nats_inbox_prefix,
		mid or nats_inbox_mbid_nuid()
	}, '.')
end

-- fresh inbox under this client's muxed request subscription
function cm.create_inbox(client, lid)
	client = unwrap(client)
	lid = lid or nats_inbox_lid_nuid()
	return client.inbox_muxed .. '.' .. lid, lid
end
local create_inbox = cm.create_inbox

function cm.new_inbox(client, prefix, mid)
	client = unwrap(client)
	return concat({
		prefix or client.inbox_prefix or nats_inbox_prefix,
		mid or nats_inbox_mbid_nuid()
	}, '.')
end

local function inbox_req_cb(client, m) -- inbox request callback and handling for wait function
	local lid = sub(m.subject, #client.inbox_muxed + 2)
	local tbl = client.inbox_table
	local ibt = tbl[lid]
	if not ibt then return end
	if ibt.expire and ibt.expire < now_of(client) then
		tbl[lid] = nil
		return
	end
	rawset(m, 'sid', client.inbox_sid)
	rawset(m, 'lid', lid)
	local s, rm = call_user(client, ibt.callback or passthru_func, m)
	if not s then report(client, 'request callback error: ' .. tostring(rm)) end
	if ibt.count then
		ibt.run = (ibt.run or 0) + 1
		if ibt.run >= ibt.count then tbl[lid] = nil end
	elseif not ibt.expire then
		tbl[lid] = nil
	end
	if s then return rm end
end

-- ---------------------------------------------------------------------------
-- Publish / subscribe
-- ---------------------------------------------------------------------------
local function msg_string(client, subject, data, reply, header)
	if data == nil then data = '' elseif type(data) ~= 'string' then data = tostring(data) end
	local hlen = 0
	if header ~= nil then
		if type(header) == 'table' then header = encode_header(header) end
		if header == '' then header = nil
		else
			if sub(header, 1, 5) ~= 'NATS/' then header = HDR_LINE .. header end
			if sub(header, -2) ~= CRLF then header = header .. CRLF end
			header = header .. CRLF
			hlen = #header
		end
	end
	local total = hlen + #data
	local maxp = client.server_information.max_payload or DEFAULT_MAX_PAYLOAD
	if total > maxp then
		client.error('maximum payload exceeded: ' .. total .. ' > max_payload ' .. maxp, 3)
	end
	if header then
		if reply then
			return 'HPUB ' .. subject .. ' ' .. reply .. ' ' .. hlen .. ' ' .. total .. CRLF .. header .. data .. CRLF
		end
		return 'HPUB ' .. subject .. ' ' .. hlen .. ' ' .. total .. CRLF .. header .. data .. CRLF
	end
	if reply then return 'PUB ' .. subject .. ' ' .. reply .. ' ' .. total .. CRLF .. data .. CRLF end
	return 'PUB ' .. subject .. ' ' .. total .. CRLF .. data .. CRLF
end
nats.msg_string = msg_string

-- keep the old table builder for backward compatibility of anybody poking at it
local function msg_stringtable(subject, data, reply, header)
	return { msg_string({ server_information = {} , error = nats.error }, subject, data, reply, header) }
end
nats.msg_stringtable = msg_stringtable

function cm.publish(client, subject, data, reply, header)
	local nc = unwrap(client)
	if nc.config.validate_subjects then
		if bad_pub_subject_cached(subject) then nc.error('invalid subject: ' .. tostring(subject), 2) end
		if reply ~= nil and bad_subject(reply) then nc.error('invalid reply subject: ' .. tostring(reply), 2) end
	end
	if header ~= nil and nc.config.headers == false then nc.error('headers are disabled on this connection', 2) end
	return guard(nc, function()
		local buf = msg_string(nc, subject, data, reply, header)
		-- false: the write failed inside a callback and was reported, not raised
		if network_write(nc, buf, true) ~= false then
			nc.stats.out_msgs = nc.stats.out_msgs + 1
		end
		return create_msg(subject, data, reply, header, client)
	end)
end

sub_line = function(subject, queue, sid)
	if queue and queue ~= '' then return 'SUB ' .. subject .. ' ' .. queue .. ' ' .. sid .. CRLF end
	return 'SUB ' .. subject .. ' ' .. sid .. CRLF
end

function cm.subscribe(client, subject, callback, queue)
	local nc = unwrap(client)
	if nc.config.validate_subjects then
		if bad_sub_subject(subject) then nc.error('invalid subject: ' .. tostring(subject), 2) end
		if queue and queue ~= '' and bad_queue(queue) then nc.error('invalid queue name: ' .. tostring(queue), 2) end
	end
	return guard(nc, function()
		local id = nc.ssid + 1
		rawset(nc, 'ssid', id)
		local sid = tostring(id)
		network_write(nc, sub_line(subject, queue, sid))
		nc.subscriptions[sid] = { subject = subject, queue = queue, callback = callback or passthru_func, delivered = 0 }
		return sid
	end)
end

-- unsubscribe(sid [, max]) - with max the server auto-unsubscribes after max msgs
function cm.unsubscribe(client, sid, max)
	local nc = unwrap(client)
	return guard(nc, function()
		local s = nc.subscriptions[sid]
		if max and max > 0 then
			if nc.connected then network_write(nc, 'UNSUB ' .. sid .. ' ' .. max .. CRLF) end
			if s then
				s.max = max
				if (s.delivered or 0) >= max then nc.subscriptions[sid] = nil end
			end
			return
		end
		if nc.connected then network_write(nc, 'UNSUB ' .. sid .. CRLF) end
		nc.subscriptions[sid] = nil
	end)
end

function cm.inbox_subscribe(client) -- the muxed request subscription is (re)created on connect
	client = unwrap(client)
	return client.inbox_sid
end

-- request(subject, callback, data, header, sid) -> published msg (async; response via wait())
function cm.request(client, subject, callback, data, header, sid)
	local nc = unwrap(client)
	local reply, lid
	if sid and nc.subscriptions[sid] then
		reply = nc.subscriptions[sid].subject
	else
		reply, lid = create_inbox(nc)
		nc.inbox_table[lid] = { created = now_of(nc), callback = callback }
	end
	local msg = cm.publish(client, subject, data, reply, header)
	rawset(msg, "sid", sid or nc.inbox_sid)
	if lid then rawset(msg, "lid", lid) end
	return msg
end

-- get(subject, data, header, sid [, timeout]) -> response msg | nil, 'timeout' | nil, 'no responders'
function cm.get(client, subject, data, header, sid, timeout)
	local nc = unwrap(client)
	-- One owner command: publish and the wait that collects the reply stay together.
	return guard(nc, function()
		local msg = cm.request(client, subject, nil, data, header, sid)
		local r_msg, err = cm.wait(client, {
			sid = msg.sid,
			lid = msg.lid,
			timeout = timeout or nc.config.timeout })
		if msg.lid then nc.inbox_table[msg.lid] = nil end
		if not r_msg then return nil, (err and err ~= 'timeout') and err or 'timeout' end
		if rawget(r_msg, 'status_code') == '503' then return nil, 'no responders', r_msg end
		return r_msg
	end)
end
cm.request_sync = cm.get

-- poll: request with many responses for `wait` seconds and/or `count` replies
function cm.poll(client, subject, callback, data, header, wait, count)
	local nc = unwrap(client)
	local reply, lid = create_inbox(nc)
	nc.inbox_table[lid] = { created = now_of(nc), callback = callback, wait = wait or 2,
		expire = now_of(nc) + (wait or 2), count = count }
	local msg = cm.publish(client, subject, data, reply, header)
	rawset(msg, "sid", nc.inbox_sid)
	rawset(msg, "lid", lid)
	return msg
end

function cm.auth(client, user, pass)
	client = unwrap(client)
	client.config.user = user or client.config.user
	client.config.pass = pass or client.config.pass
	return { user = client.config.user, pass = client.config.pass }
end

-- ---------------------------------------------------------------------------
-- Dispatch loop
-- ---------------------------------------------------------------------------
local function handle_info(nc, data)
	local ok, info = pcall(decode, data)
	if not ok or type(info) ~= 'table' then return end
	local si = nc.server_information
	for k, v in pairs(info) do si[k] = v end
	si.max_payload = floor(tonumber(si.max_payload) or DEFAULT_MAX_PAYLOAD)
	merge_discovered(nc, info.connect_urls)
	if info.ldm then -- ADR-5 lame duck mode
		rawset(nc, 'lame_duck', true)
		local cb = nc.config.lame_duck_cb
		if cb then pcall(cb, nc) end
	end
end

local function send_ping(nc)
	if not nc.connected then return end
	network_write(nc, 'PING\r\n')
	rawset(nc, 'pings_sent', nc.pings_sent + 1)
	rawset(nc, 'last_ping', now_of(nc))
end

-- keepalive + housekeeping; returns read timeout capped to the next ping
local function maintain(nc, now, rt)
	local pi = nc.config.ping_interval
	if pi and pi > 0 and nc.connected then
		local due = nc.last_ping + pi
		if now >= due then
			if nc.pings_sent - nc.pongs_received >= (nc.config.max_pings_out or 2) then
				io_failure(nc, 'stale connection')
			else
				send_ping(nc)
			end
			due = now + pi
		end
		local left = due - now
		if not rt or left < rt then rt = left end
	end
	local tk = nc._tickers
	if #tk > 0 then
		for i = #tk, 1, -1 do
			local t = tk[i]
			if t and now >= t.due then
				t.due = now + t.interval
				local ok, r = call_user(nc, t.fn, nc, now)
				if not ok then report(nc, 'ticker error: ' .. tostring(r))
				elseif r == false then table.remove(tk, i) end
			end
		end
		for i = 1, #tk do
			local left = tk[i].due - now
			if left < 0 then left = 0 end
			if not rt or left < rt then rt = left end
		end
	end
	if now - nc.last_sweep > 1 then
		rawset(nc, 'last_sweep', now)
		for lid, e in pairs(nc.inbox_table) do
			if e.expire and e.expire < now then nc.inbox_table[lid] = nil end
		end
	end
	return rt
end

-- processes one protocol op; returns msg (MSG/HMSG) or nil
local function process(nc, client, op)
	if op == OP_PING then network_write(nc, 'PONG\r\n') return end
	if op == OP_PONG then rawset(nc, 'pongs_received', nc.pongs_received + 1) return end
	if op == OP_OK then return end
	local o = op.op
	if o == 'MSG' or o == 'HMSG' then
		local stats = nc.stats
		stats.in_msgs = stats.in_msgs + 1
		stats.in_bytes = stats.in_bytes + op.size
		local msg = setmetatable({
			client = client, data = op.data, header = op.header, subject = op.subject,
			reply = op.reply, sid = op.sid, status_code = op.status, status_description = op.description,
		}, msg_metatable)
		local sid = op.sid
		if sid == nc.inbox_sid then
			local r = inbox_req_cb(nc, msg)
			if not r then r = msg end
			return r, msg
		end
		local st = nc.subscriptions[sid]
		if st then
			local d = (st.delivered or 0) + 1
			st.delivered = d
			if st.max and d >= st.max then nc.subscriptions[sid] = nil end
			local s, r = call_user(nc, st.callback, msg)
			if not s then report(nc, 'subscription callback error: ' .. tostring(r))
			elseif r then return r, msg end
		end
		return msg, msg
	elseif o == 'INFO' then handle_info(nc, op.data)
	elseif o == '-ERR' then report(nc, op.data)
	end
end

local function parse_timeout(timeout)
	local tt = type(timeout)
	if tt == 'number' then return timeout >= 0 and timeout or nil
	elseif tt == 'table' then return tonumber(timeout[1]) or tonumber(timeout[2])
	elseif tt == 'string' then local n = tonumber(timeout) return (n and n >= 0) and n or nil end
	return nil
end

-- One maintenance pass and at most one socket read. An operation already
-- parsed from a previous read is delivered without reading and without
-- maintenance, so a burst of buffered messages is not held behind the clock.
-- Returns res, msg, op when an operation was processed, or nil, err.
local function step_once(client, timeout)
	local nc = unwrap(client)
	local op = pop_op(nc)
	if not op then
		local now = now_of(nc)
		local rt = timeout
		if rt and rt < 0 then rt = 0 end
		if timeout and timeout <= 0 then rt = 0
		elseif timeout then rt = timeout end
		rt = maintain(nc, now, rt)
		local up = nc.connected
		if not up then
			if rawget(nc, '_rc') then up = rc_step(nc, false)
			elseif nc.closed or not nc.config.reconnect then
				io_error(nc, 'connection error: not connected')
				return nil, 'not connected'
			else up = io_failure(nc, 'not connected') end
			if not up then
				local rc = rawget(nc, '_rc')
				local w = rc and (rc.next_at - now_of(nc)) or 0
				if rt and rt < w then w = rt end
				if timeout and timeout <= 0 then w = 0 end
				if w > 0 then sleep_of(nc, w) end
				return nil, 'timeout'
			end
		end
		if up then
			local read_to = rt
			if timeout and (read_to == nil or timeout < read_to) then read_to = timeout end
			if read_to and read_to < 0 then read_to = 0 end
			local err
			op, err = response_read(nc, read_to)
			if not op then
				if err ~= 'timeout' then
					io_failure(nc, err)
					if not nc.connected and not rawget(nc, '_rc') then return nil, err or 'closed' end
				elseif read_to and read_to > 0 then
					-- A blocking socket already consumed this time. A socket that
					-- returns at once (nothing buffered) must not busy-spin.
					local rest = read_to - (now_of(nc) - now)
					if rest > 0 then sleep_of(nc, rest) end
				end
				return nil, 'timeout'
			end
		end
	end
	if not op then return nil, 'timeout' end
	local res, msg = process(nc, client, op)
	return res, msg, op
end

-- processes ops until stop(msg, op) is true or the deadline passes.
-- returns the callback result (true when that result was nil), or nil, err.
local function drive(client, deadline, stop)
	local nc = unwrap(client)
	while true do
		local budget
		if deadline then
			budget = deadline - now_of(nc)
			if budget <= 0 then return nil, 'timeout' end
		end
		local res, msg, op = step_once(client, budget)
		if op then
			if stop(msg, op) then
				if res == nil then return true end
				return res
			end
		else
			if not nc.connected and not rawget(nc, '_rc') then return nil, msg or 'not connected' end
			if deadline and now_of(nc) >= deadline then return nil, 'timeout' end
		end
	end
end

-- step([timeout]) - one maintenance pass and at most one read, on the owner.
-- timeout is seconds (0 polls). Returns the callback result, the message and
-- the op when an operation was processed, or nil, err ('timeout', ...).
function cm.step(client, timeout)
	local nc = unwrap(client)
	timeout = parse_timeout(timeout)
	return guard(nc, function()
		return step_once(client, timeout)
	end)
end

-- wait({sid=, lid=, maxcount=, timeout=}) - the event loop.
-- returns the matching message (lid/sid), the last message once maxcount
-- messages were processed, or nil, err on timeout / disconnect.
function cm.wait(client, opts)
	opts = opts or {}
	local nc = unwrap(client)
	local sid, lid, maxcount = opts.sid, opts.lid, opts.maxcount
	return guard(nc, function()
		local timeout = parse_timeout(opts.timeout)
		local deadline = timeout and (now_of(nc) + timeout)
		local count = 0
		local r, err = drive(client, deadline, function(msg)
			if not msg then return false end
			count = count + 1
			if lid then
				if rawget(msg, 'lid') == lid then return true end
			elseif sid and rawget(msg, 'sid') == sid then return true end
			return maxcount ~= nil and count >= maxcount
		end)
		if r == true then return nil end
		if r == nil then return nil, err end
		return r
	end)
end

-- flush: PING and process traffic until the matching PONG (true) or timeout (false)
local function await_connected(nc, deadline)
	while not nc.connected do
		if not rawget(nc, '_rc') then return false end
		if rc_step(nc, false) then return true end
		local w = nc._rc and (nc._rc.next_at - now_of(nc)) or 0
		if deadline and deadline - now_of(nc) < w then w = deadline - now_of(nc) end
		if deadline and w <= 0 and now_of(nc) >= deadline then return false end
		if w > 0 then sleep_of(nc, w) end
	end
	return true
end

function cm.flush(client, timeout)
	local nc = unwrap(client)
	return guard(nc, function()
		local t = parse_timeout(timeout or nc.config.timeout)
		local deadline = t and (now_of(nc) + t)
		if not nc.connected and rawget(nc, '_rc') then
			if not await_connected(nc, deadline) then return false end
		end
		send_ping(nc)
		local target = nc.pings_sent
		if nc.pongs_received >= target then return true end
		local r = drive(client, deadline, function(_, op)
			return op == OP_PONG and nc.pongs_received >= target
		end)
		return r ~= nil
	end)
end

-- old synchronous ping helper: true if the server answered with PONG
function cm.ping(client, timeout) return cm.flush(client, timeout) end
function cm.pong(client)
	local nc = unwrap(client)
	return guard(nc, function() request_write(nc, 'PONG\r\n') end)
end

-- drain one subscription: UNSUB, deliver what is in flight, then remove it
function cm.drain_sub(client, sid, timeout)
	local nc = unwrap(client)
	return guard(nc, function()
		local s = nc.subscriptions[sid]
		if not s then return false end
		network_write(nc, 'UNSUB ' .. sid .. CRLF)
		local ok = cm.flush(client, timeout or nc.config.drain_timeout)
		nc.subscriptions[sid] = nil
		return ok
	end)
end

-- drain the connection: unsubscribe everything, process in-flight messages and
-- pending requests, then close (ADR-40 style graceful shutdown)
function cm.drain(client, timeout)
	local nc = unwrap(client)
	return guard(nc, function()
		timeout = timeout or nc.config.drain_timeout or 30
		local deadline = now_of(nc) + timeout
		if not nc.connected then cm.close(nc) return false end
		local buf = {}
		for sid in pairs(nc.subscriptions) do
			if sid ~= nc.inbox_sid then buf[#buf + 1] = 'UNSUB ' .. sid .. CRLF end
		end
		if #buf > 0 then network_write(nc, concat(buf)) end
		local ok = cm.flush(client, timeout)
		for sid in pairs(nc.subscriptions) do nc.subscriptions[sid] = nil end
		if next(nc.inbox_table) then
			drive(client, deadline, function() return next(nc.inbox_table) == nil end)
		end
		ok = ok and next(nc.inbox_table) == nil
		cm.close(nc)
		return ok
	end)
end

local function close_now(nc)
	local was = nc.socket ~= false or rawget(nc, '_rc')
	rawset(nc, 'closed', true)
	rawset(nc, '_loop', false)
	rawset(nc, '_rc', false)
	rawset(nc, 'reconnecting', false)
	close_socket(nc)
	if was and nc.config.closed_cb then pcall(nc.config.closed_cb, nc) end
end

function cm.close(client)
	local nc = unwrap(client)
	if is_foreign(nc) and rawget(nc, '_loop_owner') ~= nil then
		rawset(nc, '_loop', false)
	end
	return guard(nc, function() close_now(nc) end)
end

function cm.shutdown(client)
	local nc = unwrap(client)
	local sock = nc.socket
	if sock and sock.shutdown then pcall(sock.shutdown, sock) end
	return cm.close(nc)
end

-- start() runs the owner on runtime.spawn. The default runtime has no spawn.
-- runtime.sleep must yield to other coroutines, or those coroutines never run.
-- On Lua 5.1 a yield cannot cross pcall. Inline step/wait/flush/get run inside
-- pcall, and so does a command the owner runs for another coroutine. sleep that
-- yields works from this loop, and fails inside those calls (reconnect backoff
-- during a queued publish). Drive step() yourself there, or use a yield-safe
-- pcall. Lua 5.2+, LuaJIT, and a host pcall that allows yields do not.
-- runtime.park(cmd) waits until the owner sets cmd.done. The caller's guard
-- then returns cmd.r1, cmd.r2, cmd.r3, or raises cmd.r1 when cmd.ok is false.
-- runtime.resume(co) is optional: after a command the owner resumes the parked
-- caller so it can queue the next call before the owner reads. Hosts that
-- schedule coroutines themselves leave resume unset.
-- runtime.wakeup(client) is optional and should interrupt the owner's sleep.
-- runtime.poll_interval (seconds, default 0.05) is the owner's read slice when
-- the command queue is empty.
function cm.start(client)
	local nc = unwrap(client)
	if rawget(nc, '_loop') then return true end
	if rawget(nc, '_owner') ~= nil or (rawget(nc, '_loop_owner') ~= nil and is_foreign(nc)) then
		nc.error('connection is in use', 2)
	end
	local spawn = nc.runtime.spawn
	if type(spawn) ~= 'function' then
		nc.error('runtime.spawn is required to start a background owner', 2)
	end
	rawset(nc, '_loop', true)
	if not rawget(nc, '_cmds') then rawset(nc, '_cmds', {}) end
	-- Hold the slot until the spawned function records its own token.
	rawset(nc, '_loop_owner', {})
	local ok, err = pcall(spawn, function()
		rawset(nc, '_loop_owner', owner_token(nc))
		local slice = tonumber(nc.runtime.poll_interval) or 0.05
		if slice < 0 then slice = 0 end
		while not nc.closed and rawget(nc, '_loop') do
			if service_one_cmd(nc) then
				-- Let the parked caller queue a follow-up (request then wait)
				-- before the owner reads. sleep(0) yields on a cooperative host.
				local sl = nc.runtime.sleep
				if type(sl) == 'function' then sl(0) end
			else
				step_once(client, slice)
				if nc.closed or not rawget(nc, '_loop') then break end
				local sl = nc.runtime.sleep
				if type(sl) ~= 'function' then break end
				sl(slice)
			end
		end
		while service_one_cmd(nc) do end
		rawset(nc, '_loop', false)
		rawset(nc, '_loop_owner', nil)
	end)
	if not ok then
		rawset(nc, '_loop', false)
		rawset(nc, '_loop_owner', nil)
		error(err, 0)
	end
	return true
end

function cm.stop(client)
	local nc = unwrap(client)
	rawset(nc, '_loop', false)
	local wake = nc.runtime.wakeup
	if type(wake) == 'function' then pcall(wake, nc) end
	return true
end

local queue_mt = { __index = {} }

-- queue_subscribe(subject [, queue_group]) -> sub. Messages are queued, up to
-- config.max_chan_len (count, default 65536). A full queue drops the newest
-- message, increments stats.dropped and reports 'subscription queue full'.
function cm.queue_subscribe(client, subject, qgroup)
	local nc = unwrap(client)
	return guard(nc, function()
		local maxn = tonumber(nc.config.max_chan_len) or (64 * 1024)
		if maxn < 1 then maxn = 64 * 1024 end
		local box = setmetatable({ msgs = {}, max = maxn, client = nc, closed = false }, queue_mt)
		box.sid = cm.subscribe(nc, subject, function(msg)
			if #box.msgs >= box.max then
				nc.stats.dropped = (nc.stats.dropped or 0) + 1
				report(nc, 'subscription queue full')
				return
			end
			box.msgs[#box.msgs + 1] = msg
		end, qgroup)
		return box
	end)
end

function queue_mt.__index.next(box, timeout)
	local nc = box.client
	local deadline = (type(timeout) == 'number') and (now_of(nc) + timeout) or nil
	while #box.msgs == 0 do
		if box.closed then return nil, 'closed' end
		local left
		if deadline then
			left = deadline - now_of(nc)
			if left <= 0 then return nil, 'timeout' end
		end
		if rawget(nc, '_loop_owner') ~= nil and is_foreign(nc) then
			local sl = nc.runtime.sleep
			if type(sl) ~= 'function' then return nil, 'timeout' end
			sl(left and min(left, 0.05) or 0.05)
		else
			local _, err, op = cm.step(nc, left)
			if not op and err and err ~= 'timeout' and #box.msgs == 0 then
				if not nc.connected and not rawget(nc, '_rc') then return nil, err end
			end
		end
	end
	return remove(box.msgs, 1)
end

function queue_mt.__index.stop(box)
	box.closed = true
	if box.sid then
		local sid = box.sid
		box.sid = nil
		pcall(cm.unsubscribe, box.client, sid)
	end
end

-- ---------------------------------------------------------------------------
-- nats msg methods
-- ---------------------------------------------------------------------------
local mm = nats.msg_metatable.methods
function cm.msg(client, subject, data, reply, header, sid)
	return create_msg(subject, data, reply, header, client, sid)
end

function mm.new(m, subject, data, reply, header, sid)
	return create_msg(subject, data, reply, header, m.client, sid)
end

function mm.clone(m, subject, data, reply, header, sid)
	return create_msg(subject or m.subject, m.data or data, m.reply or reply, m.header or header, m.client, sid or m.sid)
end

function mm.respond(m, data, reply, header)
	if m.reply then return m.client:publish(m.reply, data, reply, header) end
end

function mm.publish(m, subject, data, reply, header)
	return m.client and m.client:publish(subject or m.subject, data or m.data, reply or m.reply, header or m.header)
end

function mm.request(m, subject, callback, data, header, sid)
	return m.client and m.client:request(subject or m.subject, callback, data or m.data, header or m.header, sid or m.sid)
end

function mm.get(m, subject, data, header, sid)
	return m.client and m.client:get(subject or m.subject, data or m.data, header or m.header, sid or m.sid)
end

-- status code (number) and description of a status message (e.g. 503 no responders)
function mm.status(m)
	local st = rawget(m, 'status_code')
	if st then return tonumber(st), rawget(m, 'status_description') end
	local h = rawget(m, 'header')
	if type(h) == 'table' and h.Status then return tonumber(h.Status), h.Description end
	return nil
end

function mm.headers(m)
	local h = rawget(m, 'header')
	if type(h) == 'table' then return h end
	return decode_header(h) or {}
end

function mm.size(m)
	local client, ss, rs = m.client, #m.subject, #(m.reply or "")
	local enc_data, enc_header = client and rawget(client, 'encode_data_func'), client and rawget(client, 'encode_header_func')
	local ds, hs
	if enc_data and m.data ~= nil and type(m.data) ~= 'string' then ds = #(enc_data(m.data) or "")
	else ds = #tostring(m.data or "") end
	if enc_header and type(m.header) == 'table' then hs = #(enc_header(m.header) or "")
	else hs = #tostring(m.header or "") end
	return ss + rs + hs + ds
end

-- ---------------------------------------------------------------------------
-- encoded connection / client
-- ---------------------------------------------------------------------------
function cm.encoded_client(client, encode_data_func, decode_data_func, encode_header_func, decode_header_func)
	return setmetatable({
		client = unwrap(client),
		encode_data_func = encode_data_func or encode,
		decode_data_func = decode_data_func or decode,
		encode_header_func = encode_header_func or nats.encode_header,
		decode_header_func = decode_header_func or nats.decode_header,
	}, encoded_client_metatable)
end

local ec = encoded_client_metatable.methods
local function ec_call(client, fname, v, label)
	if v == nil then return nil end
	local s, r = pcall(rawget(client, fname), v)
	if s then return r end
	report(client.client, label .. ' error: ' .. tostring(r) .. ' : ' .. tostring(v))
end
function ec.encode_data(client, data) return ec_call(client, 'encode_data_func', data, 'encode_data') end
function ec.decode_data(client, data)
	if data == '' then return nil end
	return ec_call(client, 'decode_data_func', data, 'decode_data')
end
function ec.encode_header(client, header) return ec_call(client, 'encode_header_func', header, 'encode_header') end
function ec.decode_header(client, header) return ec_call(client, 'decode_header_func', header, 'decode_header') end
local ec_encode_data, ec_decode_data, ec_encode_header, ec_decode_header =
	ec.encode_data, ec.decode_data, ec.encode_header, ec.decode_header

local function ec_decode_msg(client, m)
	if m.data then rawset(m, "data", ec_decode_data(client, m.data)) end
	if m.header then rawset(m, "header", ec_decode_header(client, m.header)) end
	return m
end

function ec.publish(client, subject, data, reply, header)
	return cm.publish(client.client, subject, ec_encode_data(client, data), reply, ec_encode_header(client, header))
end

function ec.request(client, subject, callback, data, header, sid)
	return cm.request(client.client, subject,
		function(m)
			rawset(m, "client", client)
			local s, r = pcall(callback or passthru_func, ec_decode_msg(client, m))
			return s and r or nil
		end,
		ec_encode_data(client, data), ec_encode_header(client, header), sid)
end

function ec.get(client, subject, data, header, sid, timeout)
	local msg, err, raw = cm.get(client.client, subject, ec_encode_data(client, data), ec_encode_header(client, header), sid, timeout)
	if msg then ec_decode_msg(client, msg) end
	return msg, err, raw
end

function ec.subscribe(client, subject, callback, queue)
	return cm.subscribe(client.client, subject, function(m)
		rawset(m, "client", client)
		local s, r = pcall(callback, ec_decode_msg(client, m))
		if s then return r end
		report(client.client, 'encoded subscription callback error: ' .. tostring(r))
	end, queue)
end

function ec.poll(client, subject, callback, data, header, wait, count)
	return cm.poll(client.client, subject,
		function(m)
			rawset(m, "client", client)
			local s, r = pcall(callback, ec_decode_msg(client, m))
			if s then return r end
			report(client.client, 'encoded poll callback error: ' .. tostring(r))
		end,
		ec_encode_data(client, data), ec_encode_header(client, header), wait, count)
end


-- ---------------------------------------------------------------------------
-- Well known headers and status codes (ADR-4, ADR-9, ADR-13, ADR-31, ADR-43, ADR-50)
-- ---------------------------------------------------------------------------
nats.headers = {
	MSG_ID = 'Nats-Msg-Id',
	EXPECTED_STREAM = 'Nats-Expected-Stream',
	EXPECTED_LAST_SEQ = 'Nats-Expected-Last-Sequence',
	EXPECTED_LAST_SUBJECT_SEQ = 'Nats-Expected-Last-Subject-Sequence',
	EXPECTED_LAST_SUBJECT_SEQ_SUBJECT = 'Nats-Expected-Last-Subject-Sequence-Subject',
	EXPECTED_LAST_MSG_ID = 'Nats-Expected-Last-Msg-Id',
	ROLLUP = 'Nats-Rollup',
	STREAM = 'Nats-Stream',
	SEQUENCE = 'Nats-Sequence',
	TIMESTAMP = 'Nats-Time-Stamp',
	SUBJECT = 'Nats-Subject',
	LAST_SEQUENCE = 'Nats-Last-Sequence',
	NUM_PENDING = 'Nats-Num-Pending',
	UP_TO_SEQUENCE = 'Nats-UpTo-Sequence',
	PENDING_MESSAGES = 'Nats-Pending-Messages',
	PENDING_BYTES = 'Nats-Pending-Bytes',
	LAST_CONSUMER = 'Nats-Last-Consumer',
	LAST_STREAM = 'Nats-Last-Stream',
	CONSUMER_STALLED = 'Nats-Consumer-Stalled',
	TTL = 'Nats-TTL',
	MARKER_REASON = 'Nats-Marker-Reason',
	INCR = 'Nats-Incr',
	BATCH_ID = 'Nats-Batch-Id',
	BATCH_SEQUENCE = 'Nats-Batch-Sequence',
	BATCH_COMMIT = 'Nats-Batch-Commit',
	SCHEDULE = 'Nats-Schedule',
	SCHEDULE_TARGET = 'Nats-Schedule-Target',
	REQUIRED_API_LEVEL = 'Nats-Required-Api-Level',
	SERVICE_ERROR = 'Nats-Service-Error',
	SERVICE_ERROR_CODE = 'Nats-Service-Error-Code',
	KV_OPERATION = 'KV-Operation',
	STATUS = 'Status',
	DESCRIPTION = 'Description',
}
nats.status = {
	IDLE_HEARTBEAT = 100, -- also flow control requests (description "FlowControl Request")
	EOB = 204,            -- end of batch (direct get batch)
	BAD_REQUEST = 400,
	NOT_FOUND = 404,
	TIMEOUT = 408,
	CONFLICT = 409,       -- e.g. max waiting / consumer deleted / leadership change
	PIN_ID_MISMATCH = 423,
	NO_RESPONDERS = 503,
}

-- ---------------------------------------------------------------------------
-- Request many (ADR-47): blocking scatter/gather.
-- request_many(subject, data, opts) -> array of msgs, err
-- opts: header, max_messages, max_wait (total seconds, default timeout),
--       stall (seconds without a new message after the first, ends the request),
--       sentinel (function(msg) -> true ends; or true = empty payload without headers ends),
--       callback (function(msg) -> false ends; messages are not collected when set)
-- ---------------------------------------------------------------------------
function cm.request_many(client, subject, data, opts)
	local nc = unwrap(client)
	opts = opts or {}
	local reply, lid = create_inbox(nc)
	local out, done = {}, false
	local last = now_of(nc)
	local sentinel = opts.sentinel
	if sentinel == true then
		sentinel = function(m) return (m.data == nil or m.data == '') and not m.header end
	end
	nc.inbox_table[lid] = { created = last, expire = last + 3600, callback = function(m)
		last = now_of(nc)
		if rawget(m, 'status_code') == '503' then done = 'no responders' return m end
		if sentinel and sentinel(m) then done = true return m end
		if opts.callback then
			if opts.callback(m) == false then done = true end
		else out[#out + 1] = m end
		if opts.max_messages and #out >= opts.max_messages then done = true end
		return m
	end }
	cm.publish(client, subject, data, reply, opts.header)
	local deadline = now_of(nc) + (opts.max_wait or nc.config.timeout or 5)
	while not done do
		local now = now_of(nc)
		local stop_at = deadline
		if opts.stall and #out > 0 and last + opts.stall < stop_at then stop_at = last + opts.stall end
		if now >= stop_at then break end
		cm.wait(client, { lid = lid, timeout = stop_at - now })
	end
	nc.inbox_table[lid] = nil
	if done == 'no responders' and #out == 0 then return out, 'no responders' end
	return out
end

-- ---------------------------------------------------------------------------
-- NATS configuration contexts (ADR-21): options compatible with the nats CLI
-- context files ($XDG_CONFIG_HOME or ~/.config)/nats/context/<name>.json
-- ---------------------------------------------------------------------------
function nats.context_dir()
	local base = os.getenv('XDG_CONFIG_HOME')
	if not base or base == '' then base = (os.getenv('HOME') or '.') .. '/.config' end
	return base .. '/nats'
end

local function read_file(path)
	return nats.runtime.read_file(path)
end

-- load_context([name | path]) -> client options table, raw context (or nil, err)
function nats.load_context(name)
	local dir = nats.context_dir()
	local path
	if name and find(name, '/', 1, true) then path = name
	else
		if not name or name == '' then
			name = match(read_file(dir .. '/context.txt') or '', '^%s*(.-)%s*$')
			if not name or name == '' then return nil, 'no context selected' end
		end
		path = dir .. '/context/' .. name .. '.json'
	end
	local raw = read_file(path)
	if not raw then return nil, 'context not found: ' .. path end
	local ok, ctx = pcall(decode, raw)
	if not ok or type(ctx) ~= 'table' then return nil, 'invalid context json' end
	local function expand(p) return p and (gsub(p, '^~', os.getenv('HOME') or '~')) or nil end
	local function nz(v) if v == nil or v == '' or v == json.null then return nil end return v end
	local o = {}
	if nz(ctx.url) then o.servers = ctx.url end
	o.user, o.pass, o.auth_token = nz(ctx.user), nz(ctx.password), nz(ctx.token)
	o.creds = expand(nz(ctx.creds))
	o.nkey_file = expand(nz(ctx.nkey))
	o.inbox_prefix = nz(ctx.inbox_prefix)
	if nz(ctx.cert) or nz(ctx.key) or nz(ctx.ca) then
		o.tls = { certificate = expand(nz(ctx.cert)), key = expand(nz(ctx.key)), cafile = expand(nz(ctx.ca)),
			verify = nz(ctx.ca) and 'peer' or 'none' }
		o.tls_required = true
	end
	if ctx.tls_first then o.tls_first = true end
	o.jetstream = { domain = nz(ctx.jetstream_domain), api_prefix = nz(ctx.jetstream_api_prefix),
		event_prefix = nz(ctx.jetstream_event_prefix) }
	return o, ctx
end

function nats.connect_context(name, extra)
	local o, err = nats.load_context(name)
	if not o then return nil, err end
	for k, v in pairs(extra or {}) do o[k] = v end
	return nats.connect(o)
end

return nats
