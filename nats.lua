-- nats.lua - Lua client for NATS (https://nats.io)
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Core client: connection handling, protocol parser, pub/sub, request/reply,
-- headers, keepalive, reconnect and drain. Blocking LuaSocket I/O; messages are
-- dispatched to callbacks while client:wait() (or a request) pumps the socket.

local socket = require('socket')
local nuid = require('nuid')

local nats = {
	_VERSION     = 'lua-nats-nx 0.1.0',
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
local gettime, sleep = socket.gettime, socket.sleep

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
	version = '0.1.0',
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
	max_chan_len = true,
}
nats.valid_config = valid_config

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
	}
	for k, v in pairs(config or {}) do cfg[k] = v end

	local prefix = (cfg.inbox_prefix or '_INBOX') .. '.' .. nuid.next()
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
		inbox_prefix = prefix,  -- muxed request inbox: <prefix>.<token>
		inbox_sid = '1',
		inbox_table = {},
		ssid = 1,
		pings_sent = 0,
		pongs_received = 0,
		last_ping = 0,
		last_sweep = 0,
		lame_duck = false,
		last_error = false,
		reconnecting = false,
		stats = { in_msgs = 0, out_msgs = 0, in_bytes = 0, out_bytes = 0, reconnects = 0 },
		_partial = false,
		_pend = false,
		_to = false,
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
		local ok, unix = pcall(require, 'socket.unix')
		if not ok then return nil, 'your build of LuaSocket does not support UNIX domain sockets' end
		local s = (type(unix) == 'table' and unix.stream) and unix.stream() or unix()
		s:settimeout(timeout)
		local cok, err = s:connect(srv.path)
		if not cok then s:close() return nil, 'could not connect to ' .. srv.path .. ' [' .. tostring(err) .. ']' end
		return s
	end
	local s, err = socket.tcp()
	if not s then return nil, err end
	s:settimeout(timeout)
	local cok, cerr = s:connect(srv.host, srv.port)
	if not cok then
		s:close()
		return nil, 'could not connect to ' .. srv.host .. ':' .. srv.port .. ' [' .. tostring(cerr) .. ']'
	end
	if cfg.tcp_nodelay ~= nil then s:setoption('tcp-nodelay', cfg.tcp_nodelay and true or false) end
	return s
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

-- receive with partial-read carry over; returns data or nil, 'timeout' / err
local function raw_receive(client, pattern, timeout)
	local sock = rawget(client, 'socket')
	if not sock then return nil, 'not connected' end
	local to = timeout or -1
	if to < 0 then to = -1 end
	if rawget(client, '_to') ~= to then sock:settimeout(to) rawset(client, '_to', to) end
	local data, err, partial = sock:receive(pattern, rawget(client, '_partial') or nil)
	if data then
		rawset(client, '_partial', false)
		return data
	end
	if err == 'timeout' or err == 'wantread' then
		rawset(client, '_partial', (partial and partial ~= '') and partial or false)
		return nil, 'timeout'
	end
	rawset(client, '_partial', false)
	return nil, err or 'closed'
end

local function close_socket(client)
	local sock = rawget(client, 'socket')
	if sock then pcall(sock.close, sock) end
	rawset(client, 'socket', false)
	rawset(client, 'connected', false)
	rawset(client, '_partial', false)
	rawset(client, '_pend', false)
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

-- Reads one protocol operation. Returns an op table, or nil, 'timeout' / err.
-- MSG/HMSG ops: {op, subject, sid, reply, data, header, status, description}
local function response_read(client, timeout)
	local pend = rawget(client, '_pend')
	if not pend then
		local line, err = raw_receive(client, '*l', timeout)
		if not line then return nil, err end
		if nats.verbose then print('<<- ' .. line) end
		local c = byte(line, 1)
		if c == 77 then -- MSG <subject> <sid> [reply] <size>
			local subj, sid, a, b = match(line, '^MSG%s+(%S+)%s+(%S+)%s+(%S+)%s*(%S*)')
			if not subj then return { op = '-ERR', data = 'parser error: ' .. line } end
			if b == '' then pend = { op = 'MSG', subject = subj, sid = sid, size = tonumber(a), hsize = 0 }
			else pend = { op = 'MSG', subject = subj, sid = sid, reply = a, size = tonumber(b), hsize = 0 } end
		elseif c == 72 then -- HMSG <subject> <sid> [reply] <hdr_size> <total_size>
			local subj, sid, a, b, e = match(line, '^HMSG%s+(%S+)%s+(%S+)%s+(%S+)%s+(%S+)%s*(%S*)')
			if not subj then return { op = '-ERR', data = 'parser error: ' .. line } end
			if e == '' then pend = { op = 'HMSG', subject = subj, sid = sid, hsize = tonumber(a), size = tonumber(b) }
			else pend = { op = 'HMSG', subject = subj, sid = sid, reply = a, hsize = tonumber(b), size = tonumber(e) } end
		elseif line == 'PING' then return OP_PING
		elseif line == 'PONG' then return OP_PONG
		elseif line == '+OK' then return OP_OK
		elseif c == 73 and sub(line, 1, 5) == 'INFO ' then return { op = 'INFO', data = sub(line, 6) }
		elseif c == 45 and sub(line, 1, 4) == '-ERR' then
			local m = match(line, "^%-ERR%s+'?(.-)'?%s*$") or line
			return { op = '-ERR', data = m }
		else
			return { op = '-ERR', data = 'unknown protocol operation: ' .. line }
		end
		if not pend.size or not pend.hsize or pend.hsize > pend.size then
			return { op = '-ERR', data = 'parser error: ' .. line }
		end
		rawset(client, '_pend', pend)
	end
	local body, err = raw_receive(client, pend.size + 2, timeout)
	if not body then return nil, err end
	rawset(client, '_pend', false)
	local hs = pend.hsize
	if hs > 0 then
		pend.header, pend.status, pend.description = split_header(sub(body, 1, hs))
		pend.data = sub(body, hs + 1, -3)
	else
		pend.data = sub(body, 1, -3)
	end
	return pend
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
	hostport = match(hostport, '^([^/]*)')
	local host, port = match(hostport, '^%[(.-)%]:?(%d*)$')
	if not host then
		host, port = match(hostport, '^([^:]*):?(%d*)$')
	end
	local srv = { url = u, host = (host and host ~= '') and host or '127.0.0.1',
		port = tonumber(port) or default_port or 4222, attempts = 0 }
	if scheme == 'tls' then srv.tls = true end
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
	local known = {}
	for _, s in ipairs(pool) do known[s.host .. ':' .. s.port] = true end
	for _, u in ipairs(urls) do
		local s = parse_url(u)
		local key = s.host .. ':' .. s.port
		if not known[key] then
			s.discovered = true
			known[key] = true
			pool[#pool + 1] = s
		end
	end
end

-- ---------------------------------------------------------------------------
-- Connect / reconnect
-- ---------------------------------------------------------------------------
local cm = nats.client_metatable.methods
local client_info_response = { "verbose", "pedantic", "jwt", "nkey", "sig", "user", "pass", "auth_token",
	"tls_required", "name", "protocol", "echo", "headers", "no_responders" }

local function read_creds(creds)
	local content = creds
	if not find(creds, '-----BEGIN', 1, true) then
		local f, err = io.open(creds, 'rb')
		if not f then return nil, 'cannot read creds file: ' .. tostring(err) end
		content = f:read('*a')
		f:close()
	end
	local jwt = match(content, '%-%-%-%-%-BEGIN NATS USER JWT%-%-%-%-%-%s*([^%s%-]+)')
	local seed = match(content, '%-%-%-%-%-BEGIN USER NKEY SEED%-%-%-%-%-%s*([^%s%-]+)')
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
	if cfg.creds then
		local jwt, seed = read_creds(cfg.creds)
		if not jwt then return nil, seed end
		ct.jwt = jwt
		if info.nonce then
			if not cfg.signature_cb then return nil, 'creds auth requires a signature_cb(nonce, seed) (ed25519 signer)' end
			ct.sig = cfg.signature_cb(info.nonce, seed)
		end
	elseif cfg.nkey and info.nonce and not cfg.sig then
		if not cfg.signature_cb then return nil, 'nkey auth requires a signature_cb(nonce) (ed25519 signer)' end
		ct.sig = cfg.signature_cb(info.nonce)
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

	local line, rerr = raw_receive(client, '*l', ct)
	if not line then close_socket(client) return nil, 'no INFO from server: ' .. tostring(rerr) end
	if sub(line, 1, 5) ~= 'INFO ' then close_socket(client) return nil, 'unexpected greeting: ' .. line end
	local ok, info = pcall(decode, sub(line, 6))
	if not ok or type(info) ~= 'table' then close_socket(client) return nil, 'invalid INFO json' end
	info.max_payload = tonumber(info.max_payload) or DEFAULT_MAX_PAYLOAD

	if not tls_done and (info.tls_required or cfg.tls_required or srv.tls) then
		local tsock, terr = tls_wrap(sock, cfg, srv.host, ct)
		if not tsock then close_socket(client) return nil, terr end
		rawset(client, 'socket', tsock)
	end

	local connect, cerr = build_connect(client, srv, info)
	if not connect then close_socket(client) return nil, cerr end
	local sok, serr = raw_send(client, connect .. 'PING\r\n')
	if not sok then close_socket(client) return nil, serr end

	local deadline = gettime() + ct
	while true do
		local op, e = response_read(client, deadline - gettime())
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
	rawset(client, 'last_ping', gettime())
	srv.attempts = 0
	merge_discovered(client, info.connect_urls)

	-- (re)subscribe: inbox mux + all live subscriptions
	local buf = { 'SUB ', client.inbox_prefix, '.* ', client.inbox_sid, CRLF }
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

local function do_reconnect(client, reason)
	local cfg = client.config
	rawset(client, 'reconnecting', true)
	close_socket(client)
	if cfg.disconnected_cb then pcall(cfg.disconnected_cb, client, reason) end
	local pool = client.servers or build_pool(client)
	-- try other servers first, the one we just lost last
	local cur = client.current_server
	local order = {}
	for _, s in ipairs(pool) do if s ~= cur then order[#order + 1] = s end end
	if not cfg.no_randomize then shuffle(order) end
	if cur then order[#order + 1] = cur end

	local max = cfg.max_reconnect or 60
	local attempt, last_err = 0, reason
	while max < 0 or attempt < max * #order do
		for _, srv in ipairs(order) do
			if max >= 0 and attempt >= max * #order then break end
			if attempt >= #order then sleep(backoff(client, floor(attempt / #order), srv)) end
			attempt = attempt + 1
			srv.attempts = (srv.attempts or 0) + 1
			local ok, err = connect_once(client, srv)
			if ok then
				rawset(client, 'reconnecting', false)
				client.stats.reconnects = client.stats.reconnects + 1
				if cfg.reconnected_cb then pcall(cfg.reconnected_cb, client) end
				return true
			end
			last_err = err
		end
	end
	rawset(client, 'reconnecting', false)
	rawset(client, 'closed', true)
	if cfg.closed_cb then pcall(cfg.closed_cb, client) end
	client.error('connection error: reconnect failed (' .. tostring(last_err) .. ')', 3)
end

-- network failure: reconnect if enabled, otherwise raise
local function io_failure(client, err)
	close_socket(client)
	if client.closed or not client.config.reconnect or client.reconnecting then
		client.error('connection error: ' .. tostring(err), 3)
	end
	report(client, 'disconnected: ' .. tostring(err))
	return do_reconnect(client, err)
end

local function network_write(client, buffer)
	if nats.verbose then print('->> ' .. sub(buffer, 1, -3)) end
	local ok, err = raw_send(client, buffer)
	if not ok then
		if err == 'not connected' and not client.config.reconnect then client.error('not connected', 3) end
		io_failure(client, err)
		ok, err = raw_send(client, buffer)
		if not ok then client.error('connection error: ' .. tostring(err), 3) end
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
end

-- force a reconnect (ADR-40 on-demand reconnect)
function cm.reconnect(client)
	client = unwrap(client)
	return do_reconnect(client, 'reconnect requested')
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

-- ---------------------------------------------------------------------------
-- Inbox / request-reply (new style muxed inbox)
-- ---------------------------------------------------------------------------
local inbox_prefix = "_INBOX"
local inbox_nuid = nuid({ preLen = 22, seqLen = 0 })()
local inbox_lid_nuid = nuid({ preLen = 8, seqLen = 0 }) -- letter ids are 8 char random with no sequence

-- legacy module level inbox (not bound to a client's muxed subscription)
function nats.create_inbox()
	local lid = inbox_lid_nuid()
	return inbox_prefix .. '.' .. inbox_nuid .. '.' .. lid, lid
end

-- fresh inbox under this client's muxed request subscription
function cm.create_inbox(client)
	client = unwrap(client)
	local lid = nuid.next()
	return client.inbox_prefix .. '.' .. lid, lid
end
local create_inbox = cm.create_inbox

function cm.new_inbox() return inbox_prefix .. '.' .. nuid.next() end

local function inbox_req_cb(client, m) -- inbox request callback and handling for wait function
	local lid = sub(m.subject, #client.inbox_prefix + 2)
	local tbl = client.inbox_table
	local ibt = tbl[lid]
	if not ibt then return end
	if ibt.expire and ibt.expire < gettime() then
		tbl[lid] = nil
		return
	end
	rawset(m, 'sid', client.inbox_sid)
	rawset(m, 'lid', lid)
	local s, rm = pcall(ibt.callback or passthru_func, m)
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
	local buf = msg_string(nc, subject, data, reply, header)
	network_write(nc, buf)
	nc.stats.out_msgs = nc.stats.out_msgs + 1
	return create_msg(subject, data, reply, header, client)
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
	local id = nc.ssid + 1
	rawset(nc, 'ssid', id)
	local sid = tostring(id)
	network_write(nc, sub_line(subject, queue, sid))
	nc.subscriptions[sid] = { subject = subject, queue = queue, callback = callback or passthru_func, delivered = 0 }
	return sid
end

-- unsubscribe(sid [, max]) - with max the server auto-unsubscribes after max msgs
function cm.unsubscribe(client, sid, max)
	local nc = unwrap(client)
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
		nc.inbox_table[lid] = { created = gettime(), callback = callback }
	end
	local msg = cm.publish(client, subject, data, reply, header)
	rawset(msg, "sid", sid or nc.inbox_sid)
	if lid then rawset(msg, "lid", lid) end
	return msg
end

-- get(subject, data, header, sid [, timeout]) -> response msg | nil, 'timeout' | nil, 'no responders'
function cm.get(client, subject, data, header, sid, timeout)
	local nc = unwrap(client)
	local msg = cm.request(client, subject, nil, data, header, sid)
	local r_msg = cm.wait(client, {
		sid = msg.sid,
		lid = msg.lid,
		timeout = timeout or nc.config.timeout })
	if msg.lid then nc.inbox_table[msg.lid] = nil end
	if not r_msg then return nil, 'timeout' end
	if rawget(r_msg, 'status') == '503' then return nil, 'no responders', r_msg end
	return r_msg
end
cm.request_sync = cm.get

-- poll: request with many responses for `wait` seconds and/or `count` replies
function cm.poll(client, subject, callback, data, header, wait, count)
	local nc = unwrap(client)
	local reply, lid = create_inbox(nc)
	nc.inbox_table[lid] = { created = gettime(), callback = callback, wait = wait or 2,
		expire = gettime() + (wait or 2), count = count }
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
	si.max_payload = tonumber(si.max_payload) or DEFAULT_MAX_PAYLOAD
	merge_discovered(nc, info.connect_urls)
	if info.ldm then -- ADR-5 lame duck mode
		rawset(nc, 'lame_duck', true)
		local cb = nc.config.lame_duck_cb
		if cb then pcall(cb, nc) end
	end
end

local function send_ping(nc)
	network_write(nc, 'PING\r\n')
	rawset(nc, 'pings_sent', nc.pings_sent + 1)
	rawset(nc, 'last_ping', gettime())
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
			reply = op.reply, sid = op.sid, status = op.status, description = op.description,
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
			local s, r = pcall(st.callback, msg)
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

-- core pump: processes ops until stop(msg) is true, deadline passes or returns nil
local function pump(client, deadline, stop)
	local nc = unwrap(client)
	local rt, burst = nil, 0
	while true do
		-- fast path: data already buffered in LuaSocket -> skip clock/keepalive
		local sock = nc.socket
		if burst > 0 and burst < 256 and sock and sock.dirty and sock:dirty() then
			burst = burst + 1
		else
			burst = 1
			local now = gettime()
			rt = nil
			if deadline then
				rt = deadline - now
				if rt <= 0 then return nil, 'timeout' end
			end
			rt = maintain(nc, now, rt)
		end
		if not nc.connected then
			if nc.closed or not nc.config.reconnect then nc.error('connection error: not connected', 3) end
			io_failure(nc, 'not connected')
		end
		local op, err = response_read(nc, rt)
		if op then
			local res, msg = process(nc, client, op)
			if stop(msg, op) then
				if res == nil then return true end
				return res
			end
		elseif err ~= 'timeout' then
			io_failure(nc, err)
		end
	end
end

-- wait({sid=, lid=, maxcount=, timeout=}) - the event loop.
-- returns the matching message (lid/sid), the last message once maxcount
-- messages were processed, or nil on timeout.
function cm.wait(client, opts)
	opts = opts or {}
	local sid, lid, maxcount = opts.sid, opts.lid, opts.maxcount
	local timeout = parse_timeout(opts.timeout)
	local deadline = timeout and (gettime() + timeout)
	local count = 0
	local r = pump(client, deadline, function(msg)
		if not msg then return false end
		count = count + 1
		if lid then
			if rawget(msg, 'lid') == lid then return true end
		elseif sid and rawget(msg, 'sid') == sid then return true end
		return maxcount ~= nil and count >= maxcount
	end)
	if r == true then return nil end
	return r
end

-- flush: PING and process traffic until the matching PONG (true) or timeout (false)
function cm.flush(client, timeout)
	local nc = unwrap(client)
	send_ping(nc)
	local target = nc.pings_sent
	local t = parse_timeout(timeout or nc.config.timeout)
	if nc.pongs_received >= target then return true end
	local r = pump(client, t and gettime() + t, function(_, op)
		return op == OP_PONG and nc.pongs_received >= target
	end)
	return r ~= nil
end

-- old synchronous ping helper: true if the server answered with PONG
function cm.ping(client, timeout) return cm.flush(client, timeout) end
function cm.pong(client) request_write(unwrap(client), 'PONG\r\n') end

-- drain one subscription: UNSUB, deliver what is in flight, then remove it
function cm.drain_sub(client, sid, timeout)
	local nc = unwrap(client)
	local s = nc.subscriptions[sid]
	if not s then return false end
	network_write(nc, 'UNSUB ' .. sid .. CRLF)
	local ok = cm.flush(client, timeout or nc.config.drain_timeout)
	nc.subscriptions[sid] = nil
	return ok
end

-- drain the connection: unsubscribe everything, process in-flight messages and
-- pending requests, then close (ADR-40 style graceful shutdown)
function cm.drain(client, timeout)
	local nc = unwrap(client)
	timeout = timeout or nc.config.drain_timeout or 30
	local deadline = gettime() + timeout
	if not nc.connected then cm.close(nc) return false end
	local buf = {}
	for sid in pairs(nc.subscriptions) do
		if sid ~= nc.inbox_sid then buf[#buf + 1] = 'UNSUB ' .. sid .. CRLF end
	end
	if #buf > 0 then network_write(nc, concat(buf)) end
	local ok = cm.flush(client, timeout)
	for sid in pairs(nc.subscriptions) do nc.subscriptions[sid] = nil end
	if next(nc.inbox_table) then
		pump(client, deadline, function() return next(nc.inbox_table) == nil end)
	end
	ok = ok and next(nc.inbox_table) == nil
	cm.close(nc)
	return ok
end

function cm.close(client)
	local nc = unwrap(client)
	local was = nc.socket ~= false
	rawset(nc, 'closed', true)
	close_socket(nc)
	if was and nc.config.closed_cb then pcall(nc.config.closed_cb, nc) end
end

function cm.shutdown(client)
	local nc = unwrap(client)
	local sock = nc.socket
	if sock and sock.shutdown then pcall(sock.shutdown, sock) end
	cm.close(nc)
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
	local st = rawget(m, 'status')
	if st then return tonumber(st), rawget(m, 'description') end
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

return nats
