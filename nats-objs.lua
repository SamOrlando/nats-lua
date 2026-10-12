-- nats-objs.lua - JetStream Object Store (ADR-20) for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Wire compatible with nats.go (jetstream/object.go):
--   stream  OBJ_<bucket>, subjects $O.<bucket>.C.> and $O.<bucket>.M.>
--   chunks  $O.<bucket>.C.<object-nuid>
--   meta    $O.<bucket>.M.<base64url(name)>  (JSON ObjectInfo, Nats-Rollup: sub)
--   digest  "SHA-256=<base64url(sha256)>"
--
--   local nats = require('nats-objs')
--   local obs = nc:create_object_store({ bucket = 'files' })
--   obs:put('a.txt', 'hello'); local data, info = obs:get('a.txt')

local nats = require('nats-jetstream')
local sha256 = require('nats-sha256')
local nuid = require('nuid')

local encode, decode = nats.json.encode, nats.json.decode
local rawget, rawset, setmetatable, getmetatable = rawget, rawset, setmetatable, getmetatable
local type, pairs, ipairs, pcall, tostring, tonumber, error = type, pairs, ipairs, pcall, tostring, tonumber, error
local floor, min = math.floor, math.min
local byte, char, sub, format, find, concat = string.byte, string.char, string.sub, string.format, string.find, table.concat
local gettime = require('socket').gettime

local NS = nats.jetstream.NS
local cm = nats.client_metatable.methods
local js = nats.jetstream.stream_metatable.methods
local con = nats.jetstream.consumer_metatable.methods
local js_publish = nats.jetstream.publish

local objNameTmpl = "OBJ_%s"
local objAllChunksPreTmpl = "$O.%s.C.>"
local objAllMetaPreTmpl = "$O.%s.M.>"
local objChunksPreTmpl = "$O.%s.C.%s"
local objMetaPreTmpl = "$O.%s.M.%s"
local objDigestType = "SHA-256="
local DEFAULT_CHUNK = 128 * 1024
local ZERO_TIME = "0001-01-01T00:00:00Z"

local objs = { DEFAULT_CHUNK_SIZE = DEFAULT_CHUNK, sha256 = sha256 }
nats.jetstream.objs = objs
nats.objs = objs

local function unwrap(c) return rawget(c, 'client') or c end
local function is_null(v) return v == nil or v == nats.json.null end

-- ------------------------------------------------------------ base64url (RFC 4648 §5, padded like Go URLEncoding)
local B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
local B64_DEC = {}
for i = 1, 64 do B64_DEC[byte(B64, i)] = i - 1 end

local function b64url_encode(s)
	local out, n = {}, 0
	for i = 1, #s, 3 do
		local a, b, c = byte(s, i, i + 2)
		local v = a * 65536 + (b or 0) * 256 + (c or 0)
		local c1 = floor(v / 262144) % 64
		local c2 = floor(v / 4096) % 64
		local c3 = floor(v / 64) % 64
		local c4 = v % 64
		n = n + 1
		out[n] = sub(B64, c1 + 1, c1 + 1) .. sub(B64, c2 + 1, c2 + 1) ..
			(b and sub(B64, c3 + 1, c3 + 1) or '=') .. (c and sub(B64, c4 + 1, c4 + 1) or '=')
	end
	return concat(out)
end

local function b64url_decode(s)
	s = s:gsub('[=%s]', ''):gsub('%+', '-'):gsub('/', '_')
	local out, n = {}, 0
	for i = 1, #s, 4 do
		local a, b, c, d = byte(s, i, i + 3)
		a, b = B64_DEC[a], B64_DEC[b or -1]
		if not a or not b then return nil, 'invalid base64' end
		local cc, dd = c and B64_DEC[c], d and B64_DEC[d]
		if (c and not cc) or (d and not dd) then return nil, 'invalid base64' end
		local v = a * 262144 + b * 4096 + (cc or 0) * 64 + (dd or 0)
		n = n + 1
		out[n] = char(floor(v / 65536) % 256)
		if cc then n = n + 1; out[n] = char(floor(v / 256) % 256) end
		if dd then n = n + 1; out[n] = char(v % 256) end
	end
	return concat(out)
end
objs.b64url_encode, objs.b64url_decode = b64url_encode, b64url_decode
objs.encode_name = b64url_encode
objs.decode_name = b64url_decode

function objs.digest_value(raw) return objDigestType .. b64url_encode(raw) end

-- split a string into chunks of at most `size` bytes (used by put; exposed for tests)
function objs.chunker(data, size)
	local pos, n = 1, #data
	return function()
		if pos > n then return nil end
		local c = sub(data, pos, pos + size - 1)
		pos = pos + size
		return c
	end
end

-- ------------------------------------------------------------ object store objects
local obs_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] end,
	methods = {},
}
objs.metatable = obs_metatable
local om = obs_metatable.methods

function nats.valid_object_store_name(s) return type(s) == 'string' and s:match("^[%w%-_]+$") or nil end
local valid_bucket = nats.valid_object_store_name

local function bind(stream, bucket)
	return setmetatable({
		name = bucket,
		stream = stream,
		client = unwrap(stream.client),
		chunk_pre = format("$O.%s.C.", bucket),
		meta_pre = format("$O.%s.M.", bucket),
	}, obs_metatable)
end

local function stream_config(cfg)
	local ttl = tonumber(cfg.ttl)
	return {
		name = format(objNameTmpl, cfg.bucket),
		description = cfg.description,
		subjects = { format(objAllChunksPreTmpl, cfg.bucket), format(objAllMetaPreTmpl, cfg.bucket) },
		max_age = (ttl and ttl > 0) and floor(ttl * NS) or 0,
		max_bytes = (cfg.max_bytes and cfg.max_bytes > 0) and cfg.max_bytes or -1,
		storage = cfg.storage or 'file',
		num_replicas = (cfg.replicas and cfg.replicas > 0) and cfg.replicas or 1,
		placement = cfg.placement,
		discard = 'new',
		allow_rollup_hdrs = true,
		allow_direct = true,
		metadata = cfg.metadata,
		compression = cfg.compression and 's2' or 'none',
	}
end
objs.stream_config = stream_config

local function bucket_cfg(cfg)
	if type(cfg) == 'string' then cfg = { bucket = cfg } end
	if type(cfg) ~= 'table' or not valid_bucket(cfg.bucket) then return nil, 'invalid object store name' end
	return cfg
end

-- client:create_object_store(cfg) cfg: bucket, description, ttl (s), max_bytes, storage,
--   replicas, placement, compression (bool), metadata
function cm.create_object_store(client, cfg)
	local c, err = bucket_cfg(cfg)
	if not c then return nil, err end
	local stream, e2 = cm.stream(client, stream_config(c), nil, 'create')
	if not stream then return nil, e2 end
	return bind(stream, c.bucket)
end

function cm.update_object_store(client, cfg)
	local c, err = bucket_cfg(cfg)
	if not c then return nil, err end
	local stream, e2 = cm.stream(client, stream_config(c), nil, 'update')
	if not stream then return nil, e2 end
	return bind(stream, c.bucket)
end

-- bind to an existing bucket
function cm.object_store(client, bucket)
	if not valid_bucket(bucket) then return nil, 'invalid object store name' end
	local stream, err = cm.stream(client, format(objNameTmpl, bucket), nil, 'info')
	if not stream then return nil, err == 'stream not found' and 'object store not found' or err end
	return bind(stream, bucket)
end

-- create or update (like client:kvs())
function cm.objs(client, cfg)
	local c, err = bucket_cfg(cfg)
	if not c then return nil, err end
	local stream, e2 = cm.stream(client, stream_config(c))
	if not stream then return nil, e2 end
	return bind(stream, c.bucket)
end

function cm.delete_object_store(client, bucket)
	if not valid_bucket(bucket) then return false, 'invalid object store name' end
	return cm.delete_stream(client, format(objNameTmpl, bucket))
end

function cm.object_store_names(client)
	local names, err = cm.stream_names(client)
	if not names then return nil, err end
	local out = {}
	for _, n in ipairs(names) do
		local b = n:match('^OBJ_(.+)$')
		if b then out[#out + 1] = b end
	end
	return out
end

-- ------------------------------------------------------------ helpers
local function meta_subject(obs, name) return obs.meta_pre .. b64url_encode(name) end
local function chunk_subject(obs, id) return obs.chunk_pre .. id end

local function clean_info(info)
	for k, v in pairs(info) do if v == nats.json.null then info[k] = nil end end
	info.size = tonumber(info.size) or 0
	info.chunks = tonumber(info.chunks) or 0
	if type(info.options) ~= 'table' then info.options = nil end
	if info.options and type(info.options.link) ~= 'table' then info.options.link = nil end
	return info
end

-- serialise an ObjectInfo for the meta message (mtime is never stored)
local function info_json(info)
	local t = {}
	for k, v in pairs(info) do t[k] = v end
	t.mtime = ZERO_TIME
	if not t.deleted then t.deleted = nil end
	if t.digest == '' then t.digest = nil end
	return encode(t)
end

local function publish_meta(obs, info)
	local ack, err = js_publish(obs.client, meta_subject(obs, info.name), info_json(info),
		{ header = { ['Nats-Rollup'] = 'sub' } })
	if not ack then return nil, err end
	info.mtime = nats.timestamp()
	return info
end

local function purge_subject(obs, subject)
	return js.purge(obs.stream, subject)
end

-- ------------------------------------------------------------ info
-- get_info(name [, show_deleted]) -> ObjectInfo or nil, 'object not found'
function om.get_info(obs, name, show_deleted)
	if type(name) ~= 'string' or name == '' then return nil, 'object name required' end
	if type(show_deleted) == 'table' then show_deleted = show_deleted.show_deleted end
	local m, err = js.get_stored(obs.stream, meta_subject(obs, name))
	if not m then
		if err == 'message not found' then return nil, 'object not found' end
		return nil, err
	end
	local ok, info = pcall(decode, m.data)
	if not ok or type(info) ~= 'table' then return nil, 'bad object meta' end
	clean_info(info)
	if info.deleted and not show_deleted then return nil, 'object not found' end
	info.mtime = m:headers()['Nats-Time-Stamp'] or info.mtime
	return info
end

-- ------------------------------------------------------------ put
-- reader from string | function() -> chunk|nil | file handle
local function make_reader(data, size)
	local t = type(data)
	if data == nil then return function() return nil end end
	if t == 'string' then return objs.chunker(data, size) end
	if t == 'function' then
		local buf, done = '', false
		return function() -- re-chunk arbitrary reader output to exactly `size` bytes
			while not done and #buf < size do
				local c = data()
				if c == nil then done = true else buf = buf .. c end
			end
			if buf == '' then return nil end
			local c = sub(buf, 1, size)
			buf = sub(buf, size + 1)
			return c
		end
	end
	if (t == 'userdata' or t == 'table') and data.read then
		return function() return data:read(size) end
	end
	error('put: data must be a string, a reader function or a file handle')
end

objs._make_reader = make_reader

-- put(meta|name, data [, opts]) -> ObjectInfo | nil, err
-- meta: {name=, description=, headers={K={...}}, metadata={}, options={max_chunk_size=}}
-- opts: chunk_size, window (outstanding chunk publishes, default 32), timeout
function om.put(obs, meta, data, opts)
	if type(meta) == 'string' then meta = { name = meta } end
	opts = type(opts) == 'table' and opts or {}
	if type(meta) ~= 'table' or type(meta.name) ~= 'string' or meta.name == '' then return nil, 'bad object meta' end
	if meta.options and meta.options.link then return nil, 'link not allowed in put; use add_link' end
	local client = obs.client
	local maxp = client.server_information.max_payload or 1048576
	local size = tonumber((meta.options or {}).max_chunk_size) or tonumber(opts.chunk_size) or DEFAULT_CHUNK
	size = floor(size)
	if size <= 0 then return nil, 'invalid chunk size' end
	if size > maxp - 512 then size = maxp - 512 end -- room for protocol/header overhead

	local einfo, eerr = om.get_info(obs, meta.name, true)
	if not einfo and eerr ~= 'object not found' then return nil, eerr end

	local id = nuid.next()
	local csubj = chunk_subject(obs, id)
	local reader = make_reader(data, size)
	local h = sha256.new()
	local total, chunks, pending, failure = 0, 0, 0, nil
	local window = opts.window or 32
	local timeout = opts.timeout or client.config.timeout

	local function on_ack(m)
		pending = pending - 1
		local st = rawget(m, 'status_code')
		if st then failure = failure or ('chunk publish failed: status ' .. st) return end
		local ok, ack = pcall(decode, m.data)
		if not ok or type(ack) ~= 'table' then failure = failure or 'invalid chunk pub ack'
		elseif ack.error then failure = failure or (ack.error.description or 'chunk publish error') end
	end
	local function wait_until(limit)
		local deadline = gettime() + timeout
		while pending > limit do
			local left = deadline - gettime()
			if left <= 0 then failure = failure or 'timeout waiting for chunk acks' return end
			cm.wait(client, { maxcount = 1, timeout = left })
		end
	end

	while not failure do
		local c = reader()
		if c == nil or c == '' then break end
		h:update(c)
		total, chunks = total + #c, chunks + 1
		pending = pending + 1
		cm.request(client, csubj, on_ack, c)
		if pending >= window then wait_until(window - 1) end
	end
	if not failure then wait_until(0) end
	if failure then
		purge_subject(obs, csubj)
		return nil, failure
	end

	local mo = {}
	for k, v in pairs(meta.options or {}) do mo[k] = v end
	mo.max_chunk_size = size
	local info = {
		name = meta.name, description = meta.description, headers = meta.headers, metadata = meta.metadata,
		options = mo, bucket = obs.name, nuid = id, size = total, chunks = chunks,
		digest = objs.digest_value(h:final()),
	}
	local ok, err = publish_meta(obs, info)
	if not ok then
		purge_subject(obs, csubj)
		return nil, err
	end
	if einfo and not einfo.deleted and einfo.nuid and einfo.nuid ~= id then
		purge_subject(obs, chunk_subject(obs, einfo.nuid))
	end
	return info
end

function om.put_string(obs, name, s, opts) return om.put(obs, name, s, opts) end
om.put_bytes = om.put_string

-- put_file(path [, name]) - name defaults to the file's base name
function om.put_file(obs, path, name, opts)
	local f, err = io.open(path, 'rb')
	if not f then return nil, err end
	local meta = name or path:match('([^/\\]+)$')
	if type(meta) == 'string' then meta = { name = meta } end
	local info, perr = om.put(obs, meta, f, opts)
	f:close()
	return info, perr
end

-- ------------------------------------------------------------ get
-- ephemeral, ack-none pull consumer on one subject; calls each(msg, pending) for every message
local function read_subject(obs, subject, cfg, each, opts)
	opts = opts or {}
	cfg = cfg or {}
	cfg.filter_subject = subject
	cfg.ack_policy = 'none'
	cfg.deliver_policy = cfg.deliver_policy or 'all'
	cfg.mem_storage = true
	cfg.num_replicas = 1
	cfg.inactive_threshold = cfg.inactive_threshold or 30 * NS
	local consumer, err = nats.jetstream.stream_metatable.methods.consumer(obs.stream, cfg, 'create')
	if not consumer then
		return nil, type(err) == 'table' and (err.data.error and err.data.error.description) or err
	end
	local ok, result, rerr = pcall(each, consumer)
	pcall(con.delete, consumer)
	if not ok then return nil, result end
	return result, rerr
end

local function pending_of(m)
	local meta = m:metafields()
	return meta.pending
end

-- get(name [, opts]) -> data, info | nil, err
-- opts.callback(chunk) / opts.writer (file handle) stream the object instead of
-- returning it; opts.batch (chunks per pull), opts.timeout (per pull, seconds)
function om.get(obs, name, opts)
	opts = type(opts) == 'table' and opts or {}
	local info, err = om.get_info(obs, name)
	if not info then return nil, err end
	local link = info.options and info.options.link
	if link then
		if is_null(link.name) or link.name == '' then return nil, 'cannot get a bucket link' end
		local other = obs
		if link.bucket ~= obs.name then
			other, err = cm.object_store(obs.client, link.bucket)
			if not other then return nil, err end
		end
		return om.get(other, link.name, opts)
	end

	local sink, parts = opts.callback, nil
	if not sink and opts.writer then
		local w = opts.writer
		sink = function(c) w:write(c) end
	end
	if not sink then
		parts = {}
		sink = function(c) parts[#parts + 1] = c end
	end

	local h, total, got = sha256.new(), 0, 0
	if info.chunks > 0 then
		local batch = opts.batch or 64
		local to = opts.timeout or 5
		local ok, rerr = read_subject(obs, chunk_subject(obs, info.nuid), nil, function(consumer)
			while got < info.chunks do
				local msgs, ferr = con.fetch(consumer, min(batch, info.chunks - got), nil, to)
				if #msgs == 0 then error(ferr or 'timeout reading chunks', 0) end
				for _, m in ipairs(msgs) do
					local c = m.data
					h:update(c)
					total = total + #c
					got = got + 1
					local s, e = pcall(sink, c)
					if not s then error(e, 0) end
				end
			end
			return true
		end)
		if not ok then return nil, rerr end
	end
	if got ~= info.chunks then return nil, 'chunk count mismatch' end
	if total ~= info.size then return nil, 'object size mismatch' end
	if info.digest and info.digest ~= '' then
		local want = info.digest
		if want:sub(1, #objDigestType) ~= objDigestType then return nil, 'unsupported digest ' .. want end
		local got_d = objs.digest_value(h:final())
		-- compare raw bytes so padding / alphabet differences don't matter
		if b64url_decode(want:sub(#objDigestType + 1)) ~= b64url_decode(got_d:sub(#objDigestType + 1)) then
			return nil, 'object digest mismatch'
		end
	end
	if parts then return concat(parts), info end
	return true, info
end

function om.get_string(obs, name, opts) return om.get(obs, name, opts) end
om.get_bytes = om.get_string

function om.get_file(obs, name, path, opts)
	local f, err = io.open(path, 'wb')
	if not f then return nil, err end
	opts = type(opts) == 'table' and opts or {}
	opts.writer = f
	local ok, info = om.get(obs, name, opts)
	f:close()
	if not ok then os.remove(path) return nil, info end
	return info
end

-- ------------------------------------------------------------ update / delete
-- update_meta(name, meta): name, description, headers, metadata (rename when meta.name differs)
function om.update_meta(obs, name, meta)
	if type(meta) ~= 'table' then return nil, 'bad object meta' end
	local info, err = om.get_info(obs, name)
	if not info then
		if err == 'object not found' then return nil, 'cannot update meta for a deleted or missing object' end
		return nil, err
	end
	local new_name = meta.name or name
	if new_name ~= name then
		local existing = om.get_info(obs, new_name)
		if existing then return nil, 'an object already exists with that name' end
	end
	info.name = new_name
	info.description = meta.description
	info.headers = meta.headers
	info.metadata = meta.metadata
	info.mtime = nil
	local ok, perr = publish_meta(obs, info)
	if not ok then return nil, perr end
	if new_name ~= name then purge_subject(obs, meta_subject(obs, name)) end
	return info
end

-- delete(name): rollup a deleted marker and purge the chunks
function om.delete(obs, name)
	local info, err = om.get_info(obs, name, true)
	if not info then return nil, err end
	if not info.nuid or info.nuid == '' then return nil, 'bad object meta' end
	info.deleted = true
	info.size, info.chunks, info.digest = 0, 0, nil
	info.mtime = nil
	local ok, perr = publish_meta(obs, info)
	if not ok then return nil, perr end
	purge_subject(obs, chunk_subject(obs, info.nuid))
	return info
end

-- ------------------------------------------------------------ links
-- add_link(name, info) - link to an object (from this or another bucket)
function om.add_link(obs, name, target)
	if type(name) ~= 'string' or name == '' then return nil, 'link name required' end
	if type(target) ~= 'table' or not target.name then return nil, 'object info required' end
	if target.deleted then return nil, 'cannot link to a deleted object' end
	if target.options and target.options.link then return nil, 'cannot link to a link' end
	local einfo = om.get_info(obs, name, true)
	if einfo and not einfo.deleted and not (einfo.options and einfo.options.link) then
		return nil, 'an object already exists with that name'
	end
	local info = { name = name, bucket = obs.name, nuid = nuid.next(), size = 0, chunks = 0,
		options = { link = { bucket = target.bucket, name = target.name } } }
	return publish_meta(obs, info)
end

-- add_bucket_link(name, other_object_store)
function om.add_bucket_link(obs, name, other)
	if type(name) ~= 'string' or name == '' then return nil, 'link name required' end
	if type(other) ~= 'table' or not other.name then return nil, 'object store required' end
	local einfo = om.get_info(obs, name, true)
	if einfo and not einfo.deleted and not (einfo.options and einfo.options.link) then
		return nil, 'an object already exists with that name'
	end
	local info = { name = name, bucket = obs.name, nuid = nuid.next(), size = 0, chunks = 0,
		options = { link = { bucket = other.name } } }
	return publish_meta(obs, info)
end

-- ------------------------------------------------------------ list / watch
local function meta_from_msg(m)
	local ok, info = pcall(decode, m.data)
	if not ok or type(info) ~= 'table' then return nil end
	clean_info(info)
	local meta = m:metafields()
	if meta.timestamp then
		local secs = floor(meta.timestamp / NS)
		info.mtime = nats.timestamp(secs)
	end
	return info
end

-- list([{show_deleted=true}]) -> array of ObjectInfo
function om.list(obs, opts)
	opts = type(opts) == 'table' and opts or {}
	local out = {}
	local ok, err = read_subject(obs, format(objAllMetaPreTmpl, obs.name), { deliver_policy = 'last_per_subject' },
		function(consumer)
			local left = tonumber(consumer.num_pending) or 0
			while left > 0 do
				local msgs, ferr = con.fetch(consumer, min(left, 256), nil, opts.timeout or 5)
				if #msgs == 0 then error(ferr or 'timeout listing objects', 0) end
				for _, m in ipairs(msgs) do
					left = left - 1
					local info = meta_from_msg(m)
					if info and (opts.show_deleted or not info.deleted) then out[#out + 1] = info end
					if pending_of(m) == 0 then left = 0 end
				end
			end
			return true
		end)
	if not ok then return nil, err end
	return out
end

-- watcher(callback(info|nil) [, opts]) -> watcher (non blocking, ADR-20 Watch)
-- Built on an ordered consumer; updates arrive while the client pumps (client:wait()).
-- A nil info is delivered once after the initial values (end of initial data).
-- opts: include_history, ignore_deletes, updates_only
local ObjWatcher = {}
ObjWatcher.__index = ObjWatcher
function om.watcher(obs, callback, opts)
	if type(callback) ~= 'function' then return nil, 'callback required' end
	opts = type(opts) == 'table' and opts or {}
	local policy = opts.updates_only and 'new' or (opts.include_history and 'all' or 'last_per_subject')
	local w = setmetatable({ obs = obs, init_done = false, client = unwrap(obs.client) }, ObjWatcher)
	local function done()
		if not w.init_done then
			w.init_done = true
			pcall(callback, nil)
		end
	end
	local oc = js.ordered_consumer(obs.stream, { filter_subject = format(objAllMetaPreTmpl, obs.name),
		deliver_policy = policy })
	w.ordered = oc
	local ctx, err = oc:consume(function(m)
		local info = meta_from_msg(m)
		if info and not (opts.ignore_deletes and info.deleted) then
			local ok, cerr = pcall(callback, info)
			if not ok then w.last_error = cerr end
		end
		if not w.init_done and (pending_of(m) or 0) == 0 then done() end
	end, { max_messages = 256 })
	if not ctx then return nil, err end
	w.ctx = ctx
	local c = oc.consumer
	if opts.updates_only or (c and (tonumber(c.num_pending) or 0) == 0) then done() end
	return w
end
function ObjWatcher:stop() if self.ctx then self.ctx.stop() self.ctx = nil end end
ObjWatcher.unsubscribe = ObjWatcher.stop
function ObjWatcher:wait_initial(timeout)
	local deadline = gettime() + (timeout or 10)
	while not self.init_done do
		local left = deadline - gettime()
		if left <= 0 then return false end
		cm.wait(self.client, { timeout = min(left, 0.5), maxcount = 1 })
	end
	return true
end

-- watch(callback(info|nil) [, opts]) - BLOCKING convenience (0.2.0 API): runs a watcher
-- for opts.timeout seconds (default 5) or until callback returns false.
function om.watch(obs, callback, opts)
	if type(callback) ~= 'function' then return nil, 'callback required' end
	opts = type(opts) == 'table' and opts or {}
	local stop = false
	local w, err = om.watcher(obs, function(info)
		if callback(info) == false then stop = true end
	end, opts)
	if not w then return nil, err end
	local deadline = gettime() + (opts.timeout or 5)
	while not stop do
		local left = deadline - gettime()
		if left <= 0 then break end
		cm.wait(w.client, { timeout = min(left, 0.25), maxcount = 1 })
	end
	w:stop()
	return true
end

-- ------------------------------------------------------------ bucket ops
function om.seal(obs)
	local stream, err = js.info(obs.stream)
	if not stream then return nil, err end
	local cfg = {}
	for k, v in pairs(stream.config) do cfg[k] = v end
	cfg.sealed = true
	local s2, e2 = cm.stream(obs.client, cfg, nil, 'update')
	if not s2 then return nil, e2 end
	obs.stream = s2
	return om.status(obs)
end

function om.status(obs)
	local stream, err = js.info(obs.stream)
	if not stream then return nil, err end
	obs.stream = stream
	local c = stream.config
	return {
		bucket = obs.name,
		description = c.description,
		metadata = c.metadata,
		ttl = (tonumber(c.max_age) or 0) / NS,
		storage = c.storage,
		replicas = c.num_replicas,
		sealed = c.sealed == true,
		size = stream.state.bytes,
		backing_store = 'JetStream',
		is_compressed = c.compression == 's2',
		stream = stream,
	}
end

function om.destroy(obs) return js.delete(obs.stream) end

return nats
