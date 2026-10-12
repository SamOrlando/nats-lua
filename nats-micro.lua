-- nats-micro.lua - NATS Service API (ADR-32) for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
--   local nats = require('nats-micro')
--   local svc = nc:add_service({ name = 'calc', version = '1.0.0', description = 'adds' })
--   svc:add_endpoint('add', function(req) req:respond(tostring(a + b)) end)
--   local g = svc:add_group('math'); g:add_endpoint('mul', handler, { metadata = { x = 'y' } })
--   nc:wait()   -- requests are dispatched while the client pumps
--
-- Discovery: $SRV.PING|INFO|STATS[.<name>[.<id>]] with io.nats.micro.v1.*_response
-- payloads, compatible with nats.go/micro and `nats micro`.

local nats = require('nats')
local socket = require('socket')
local gettime = socket.gettime
local encode, decode = nats.json.encode, nats.json.decode
local type, pairs, ipairs, pcall, tostring, tonumber, setmetatable = type, pairs, ipairs, pcall, tostring, tonumber,
	setmetatable
local floor, format, match, find = math.floor, string.format, string.match, string.find

local cm = nats.client_metatable.methods
-- empty JSON arrays: neither cjson 2.1.0 nor the fallback codec can express [] for {}
local EMPTY = '\0__nats_empty_array__'
local function encode_body(t) return (encode(t):gsub('"\\u0000__nats_empty_array__"', '[]')) end
local function unwrap(c) return rawget(c, 'client') or c end

local micro = {
	API_PREFIX = '$SRV',
	DEFAULT_QUEUE_GROUP = 'q',
	PING_RESPONSE_TYPE = 'io.nats.micro.v1.ping_response',
	INFO_RESPONSE_TYPE = 'io.nats.micro.v1.info_response',
	STATS_RESPONSE_TYPE = 'io.nats.micro.v1.stats_response',
	ERROR_HEADER = 'Nats-Service-Error',
	ERROR_CODE_HEADER = 'Nats-Service-Error-Code',
}
nats.micro = micro

local function iso_now()
	local t = gettime()
	local s = floor(t)
	return os.date('!%Y-%m-%dT%H:%M:%S', s) .. format('.%03dZ', floor((t - s) * 1000))
end

function micro.valid_name(n) return type(n) == 'string' and match(n, '^[%w%-_]+$') ~= nil end

-- semver 2.0.0 (official grammar, implemented without regex)
function micro.valid_version(v)
	if type(v) ~= 'string' then return false end
	local core, rest = match(v, '^(%d+%.%d+%.%d+)(.*)$')
	if not core then return false end
	for part in core:gmatch('%d+') do
		if #part > 1 and part:sub(1, 1) == '0' then return false end
	end
	local pre, build = rest, nil
	local plus = find(rest, '+', 1, true)
	if plus then pre, build = rest:sub(1, plus - 1), rest:sub(plus + 1) end
	if pre ~= '' then
		if pre:sub(1, 1) ~= '-' then return false end
		pre = pre:sub(2)
		if pre == '' then return false end
		for id in (pre .. '.'):gmatch('([^.]*)%.') do
			if id == '' or not match(id, '^[%w%-]+$') then return false end
			if match(id, '^%d+$') and #id > 1 and id:sub(1, 1) == '0' then return false end
		end
	end
	if build then
		if build == '' then return false end
		for id in (build .. '.'):gmatch('([^.]*)%.') do
			if id == '' or not match(id, '^[%w%-]+$') then return false end
		end
	end
	return true
end

-- headers for a service error response
function micro.error_headers(code, description)
	return { [micro.ERROR_HEADER] = tostring(description or ''), [micro.ERROR_CODE_HEADER] = format('%d', code or 500) }
end

-- ---------------------------------------------------------------- request wrapper
local Request = {}
Request.__index = function(t, k)
	local v = Request[k]
	if v ~= nil then return v end
	return rawget(t, 'msg')[k]
end
micro.request_metatable = Request

function Request:respond(data, header)
	if not self.msg.reply then return false end
	self.responded = true
	cm.publish(self.client, self.msg.reply, data, nil, header)
	return true
end
function Request:respond_json(v, header) return self:respond(encode(v), header) end
function Request:respond_error(code, description, data, header)
	local h = {}
	for k, v in pairs(type(header) == 'table' and header or {}) do h[k] = v end
	for k, v in pairs(micro.error_headers(code, description)) do h[k] = v end
	self.error = format('%d:%s', code or 500, tostring(description or ''))
	return self:respond(data or '', h)
end
-- req.data, req.subject, req.reply, req.header are the request message fields
function Request:headers() return self.msg:headers() end

-- ---------------------------------------------------------------- service
local Service = {}
Service.__index = Service
local Group = {}
Group.__index = Group
micro.service_metatable, micro.group_metatable = Service, Group
micro.encode_body = encode_body

local function new_stats(ep)
	ep.num_requests, ep.num_errors, ep.last_error, ep.processing_time = 0, 0, '', 0
end

local function copy(t)
	if type(t) ~= 'table' then return t end
	local r = {}
	for k, v in pairs(t) do r[k] = v end
	return r
end

local function resolve_queue(own, parent)
	if own == false then return false end -- explicit "no queue group"
	if own ~= nil then return own end
	return parent
end

-- client:add_service(cfg) cfg: name, version, description, metadata, queue_group (false = none),
--   stats_handler(endpoint) -> data, done_cb(service, err), error_cb(service, err)
function cm.add_service(client, cfg)
	cfg = type(cfg) == 'table' and cfg or {}
	if not micro.valid_name(cfg.name) then return nil, 'invalid service name' end
	if not micro.valid_version(cfg.version) then return nil, 'invalid service version (semver required)' end
	local nc = unwrap(client)
	local svc = setmetatable({
		client = nc, name = cfg.name, version = cfg.version, description = cfg.description or '',
		metadata = copy(cfg.metadata) or {}, id = nats.nuid.next(), queue_group = resolve_queue(cfg.queue_group, 'q'),
		stats_handler = cfg.stats_handler, done_cb = cfg.done_cb, error_cb = cfg.error_cb,
		endpoints = {}, subs = {}, started = iso_now(), stopped = false,
	}, Service)
	for _, verb in ipairs({ 'PING', 'INFO', 'STATS' }) do
		local handler = function(m)
			local body
			if verb == 'PING' then body = svc:ping_response()
			elseif verb == 'INFO' then body = svc:info()
			else body = svc:stats() end
			if m.reply then cm.publish(nc, m.reply, encode_body(body)) end
			return nil
		end
		for _, subj in ipairs({ micro.API_PREFIX .. '.' .. verb, micro.API_PREFIX .. '.' .. verb .. '.' .. svc.name,
			micro.API_PREFIX .. '.' .. verb .. '.' .. svc.name .. '.' .. svc.id }) do
			svc.subs[#svc.subs + 1] = cm.subscribe(nc, subj, handler)
		end
	end
	if cfg.endpoint then -- optional single root endpoint
		local e = cfg.endpoint
		local ok, err = svc:add_endpoint(e.name or 'default', e.handler, e)
		if not ok then svc:stop() return nil, err end
	end
	return svc
end
nats.add_service = function(client, cfg) return cm.add_service(client, cfg) end

-- add_endpoint(name, handler(req), opts) opts: subject, metadata, queue_group (false = none)
function Service:add_endpoint(name, handler, opts, prefix, parent_queue)
	if self.stopped then return nil, 'service stopped' end
	if type(handler) ~= 'function' then return nil, 'handler required' end
	if not micro.valid_name(name) then return nil, 'invalid endpoint name' end
	opts = type(opts) == 'table' and opts or {}
	local subject = opts.subject or name
	if prefix and prefix ~= '' then subject = prefix .. '.' .. subject end
	if not nats.valid_subject(subject) then return nil, 'invalid endpoint subject' end
	local queue = resolve_queue(opts.queue_group, resolve_queue(parent_queue, self.queue_group))
	local ep = { name = name, subject = subject, queue_group = queue, metadata = copy(opts.metadata) or {},
		handler = handler, service = self }
	new_stats(ep)
	local svc = self
	ep.sid = cm.subscribe(self.client, subject, function(m)
		local req = setmetatable({ msg = m, client = svc.client, endpoint = ep }, Request)
		local t0 = gettime()
		local ok, err = pcall(handler, req)
		ep.num_requests = ep.num_requests + 1
		ep.processing_time = ep.processing_time + floor((gettime() - t0) * 1e9)
		if not ok then
			req.error = '500:' .. tostring(err)
			if not req.responded then req:respond_error(500, tostring(err)) end
			if svc.error_cb then pcall(svc.error_cb, svc, err) end
		end
		if req.error then
			ep.num_errors = ep.num_errors + 1
			ep.last_error = req.error
		end
		return nil
	end, queue or nil)
	self.endpoints[#self.endpoints + 1] = ep
	return ep
end

function Service:add_group(name, opts)
	if name ~= '' and (not nats.valid_subject(name) or find(name, '>', 1, true)) then
		return nil, 'invalid group name'
	end
	opts = type(opts) == 'table' and opts or {}
	return setmetatable({ service = self, prefix = name, queue_group = resolve_queue(opts.queue_group, nil) }, Group)
end

function Group:add_endpoint(name, handler, opts)
	return self.service:add_endpoint(name, handler, opts, self.prefix, resolve_queue(self.queue_group, nil))
end
function Group:add_group(name, opts)
	opts = type(opts) == 'table' and opts or {}
	local prefix = self.prefix ~= '' and (self.prefix .. '.' .. name) or name
	local g, err = self.service:add_group(prefix, { queue_group = resolve_queue(opts.queue_group, self.queue_group) })
	return g, err
end

local function ep_info(ep)
	return { name = ep.name, subject = ep.subject, queue_group = ep.queue_group or '',
		metadata = next(ep.metadata) and ep.metadata or nil }
end

function Service:ping_response()
	return { type = micro.PING_RESPONSE_TYPE, name = self.name, id = self.id, version = self.version,
		metadata = self.metadata }
end

function Service:info()
	local eps = {}
	for i, ep in ipairs(self.endpoints) do eps[i] = ep_info(ep) end
	local r = { type = micro.INFO_RESPONSE_TYPE, name = self.name, id = self.id, version = self.version,
		metadata = self.metadata, description = self.description, endpoints = eps }
	if #eps == 0 then r.endpoints = EMPTY end
	return r
end

function Service:stats()
	local eps = {}
	if #self.endpoints == 0 then eps = EMPTY end
	for i, ep in ipairs(self.endpoints) do
		local s = ep_info(ep) ---@diagnostic disable-line
		s.num_requests, s.num_errors, s.last_error = ep.num_requests, ep.num_errors, ep.last_error
		s.processing_time = ep.processing_time
		s.average_processing_time = ep.num_requests > 0 and floor(ep.processing_time / ep.num_requests) or 0
		if self.stats_handler then
			local ok, d = pcall(self.stats_handler, ep)
			if ok then s.data = d end
		end
		eps[i] = s
	end
	return { type = micro.STATS_RESPONSE_TYPE, name = self.name, id = self.id, version = self.version,
		metadata = self.metadata, started = self.started, endpoints = eps }
end

function Service:reset()
	for _, ep in ipairs(self.endpoints) do new_stats(ep) end
	self.started = iso_now()
end

-- stop([err]): drains the service subscriptions and calls done_cb(service, err)
function Service:stop(err)
	if self.stopped then return true end
	self.stopped = true
	local nc = self.client
	for _, sid in ipairs(self.subs) do pcall(cm.unsubscribe, nc, sid) end
	for _, ep in ipairs(self.endpoints) do pcall(cm.unsubscribe, nc, ep.sid) end
	if nc.connected then pcall(cm.flush, nc, 2) end
	if self.done_cb then pcall(self.done_cb, self, err) end
	return true
end
function Service:is_stopped() return self.stopped end

-- ---------------------------------------------------------------- discovery client
local function discover(client, verb, name, id, opts)
	opts = opts or {}
	local subj = micro.API_PREFIX .. '.' .. verb
	if name then subj = subj .. '.' .. name end
	if id then subj = subj .. '.' .. id end
	local msgs, err = cm.request_many(client, subj, '', {
		max_wait = opts.timeout or 2, stall = opts.stall or 0.3, max_messages = opts.max })
	if err and #msgs == 0 then return nil, err end
	local out = {}
	for _, m in ipairs(msgs) do
		local ok, d = pcall(decode, m.data)
		if ok and type(d) == 'table' then out[#out + 1] = d end
	end
	return out
end
function micro.ping(client, name, id, opts) return discover(client, 'PING', name, id, opts) end
function micro.info(client, name, id, opts) return discover(client, 'INFO', name, id, opts) end
function micro.stats(client, name, id, opts) return discover(client, 'STATS', name, id, opts) end
function cm.services_ping(client, name, id, opts) return micro.ping(client, name, id, opts) end
function cm.services_info(client, name, id, opts) return micro.info(client, name, id, opts) end
function cm.services_stats(client, name, id, opts) return micro.stats(client, name, id, opts) end

-- helper for callers: (code, description) if msg is a service error response
function micro.response_error(m)
	local h = m:headers()
	local code = h[micro.ERROR_CODE_HEADER]
	if code then return tonumber(code), h[micro.ERROR_HEADER] end
	return nil
end

return nats
