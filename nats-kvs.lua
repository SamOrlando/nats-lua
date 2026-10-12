-- nats-kvs.lua - JetStream Key-Value store (ADR-8, ADR-48, ADR-57) for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Buckets are streams KV_<bucket> on $KV.<bucket>.>. Gets use direct get,
-- watchers/keys/history use ordered consumers (ADR-17) that are driven by the
-- client's pump loop (client:wait()); keys()/history()/purge_deletes() pump
-- internally until the end of the initial data.

-- ### NATS-LUA Table
local nats = require('nats-jetstream')

-- ### Local properties ###
local encode, decode = nats.json.encode, nats.json.decode
local rawget, rawset = rawget, rawset
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber, tostring, pcall, pairs, ipairs = tonumber, tostring, pcall, pairs, ipairs
local type = type
local floor = math.floor
local match, format, find, sub = string.match, string.format, string.find, string.sub
local gettime = require('socket').gettime

local NS = nats.jetstream.NS
local js = nats.jetstream.stream_metatable.methods
local mm = nats.msg_metatable.methods

-- constants
local kvBucketNameTmpl = "KV_%s"
local kvSubjectsTmpl = "$KV.%s.>"
local kvSubjectsPreTmpl = "$KV.%s."
local KeyValueMaxHistory = 64
local kvop = "KV-Operation"
local kvdel = "DEL"
local kvpurge = "PURGE"
local DEFAULT_DELETE_MARKERS_OLDER_THAN = 30 * 60 -- seconds

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client meta methods
local function unwrap(c) return rawget(c, 'client') or c end

-- kvs is the implementation of KeyValueStore
nats.kvs = {
	metatable = {
		__index = function(self, key) return rawget(getmetatable(self), "methods")[key] end,
		methods = {},
	}
}
local kvs_metatable = nats.kvs.metatable
local kvsm = nats.kvs.metatable.methods

-- ADR-6: kv-bucket-name = restricted-term, kv-key-name = limited-term (dot limited-term)*
function nats.valid_bucket_name(s) return type(s) == 'string' and match(s, "^[%w%-_]+$") or nil end
local valid_bucket_name = nats.valid_bucket_name
function nats.valid_key_name(s)
	if type(s) ~= 'string' or not match(s, "^[-/_=.%w]+$") then return nil end
	if find(s, "^%.") or find(s, "%.$") or find(s, "..", 1, true) then return nil end
	return s
end
local valid_key_name = nats.valid_key_name
-- watch/keys filters may use wildcards
local function valid_key_filter(s)
	if s == '>' or s == '*' then return true end
	if type(s) ~= 'string' or not match(s, "^[-/_=.%w*>]+$") then return false end
	if find(s, "^%.") or find(s, "%.$") or find(s, "..", 1, true) then return false end
	return true
end

local function kv_stream_name(b)
	if sub(b, 1, 3) == 'KV_' then return b end
	return format(kvBucketNameTmpl, b)
end
local function strip_kv(n) return (sub(n, 1, 3) == 'KV_') and sub(n, 4) or n end

-- KeyValueConfig -> stream config (ADR-8)
-- cfg: bucket, description, history (1..64), ttl (s), max_bytes, max_value_size, storage,
--      replicas, placement, republish, mirror, sources, compression (bool or 's2'),
--      metadata, limit_marker_ttl (s, ADR-48)
local function stream_config(kvs_cfg)
	local ttl = tonumber(kvs_cfg.ttl)
	local history = tonumber(kvs_cfg.history) or 1
	if history < 1 or history > KeyValueMaxHistory then return nil, 'history must be between 1 and 64' end
	local sc = {
		name = kv_stream_name(kvs_cfg.bucket),
		description = kvs_cfg.description,
		subjects = { format(kvSubjectsTmpl, kvs_cfg.bucket) },
		max_msgs_per_subject = history,
		max_bytes = (kvs_cfg.max_bytes and kvs_cfg.max_bytes > 0) and kvs_cfg.max_bytes or -1,
		max_age = (ttl and ttl > 0) and floor(ttl * NS) or 0,
		max_msg_size = (kvs_cfg.max_value_size and kvs_cfg.max_value_size > 0) and kvs_cfg.max_value_size or -1,
		max_msgs = -1,
		max_consumers = -1,
		storage = kvs_cfg.storage or 'file',
		num_replicas = (kvs_cfg.replicas and kvs_cfg.replicas > 0) and kvs_cfg.replicas or 1,
		discard = 'new',
		allow_rollup_hdrs = true,
		deny_delete = true,
		deny_purge = false,
		allow_direct = true,
		retention = 'limits',
		placement = kvs_cfg.placement,
		republish = kvs_cfg.republish,
		metadata = kvs_cfg.metadata,
	}
	if ttl and ttl > 0 then
		sc.duplicate_window = (ttl <= 120) and floor(ttl * NS) or 120 * NS
	end
	if kvs_cfg.compression == true or kvs_cfg.compression == 's2' then sc.compression = 's2' end
	local lm = tonumber(kvs_cfg.limit_marker_ttl)
	if lm and lm > 0 then
		if lm < 1 then return nil, 'limit_marker_ttl must be at least 1 second' end
		sc.allow_msg_ttl = true
		sc.subject_delete_marker_ttl = floor(lm * NS)
	end
	-- ADR-57 mirrors and sources
	if kvs_cfg.mirror then
		local m = {}
		for k, v in pairs(kvs_cfg.mirror) do m[k] = v end
		m.name = kv_stream_name(m.name or m.bucket)
		m.bucket = nil
		sc.mirror = m
		sc.mirror_direct = true
		sc.subjects = nil
	elseif kvs_cfg.sources then
		local srcs = {}
		for i, s in ipairs(kvs_cfg.sources) do
			local c = {}
			for k, v in pairs(s) do c[k] = v end
			c.bucket = nil
			if not c.subject_transforms then
				local src = strip_kv(s.name or s.bucket)
				c.name = kv_stream_name(src)
				local f = s.filter_subject or '>'
				c.subject_transforms = { {
					src = format(kvSubjectsPreTmpl, src) .. f,
					dest = format(kvSubjectsPreTmpl, kvs_cfg.bucket) .. f,
				} }
				c.filter_subject = nil
			end
			srcs[i] = c
		end
		sc.sources = srcs
	end
	return sc
end
nats.kvs.stream_config = stream_config

local function bind(stream, bucket)
	local cfg = stream.config or {}
	local kv = setmetatable({
		name = bucket,
		bucket = bucket,
		stream = stream,
		client = stream.client,
		pre = format(kvSubjectsPreTmpl, bucket),      -- read subject prefix
		put_pre = format(kvSubjectsPreTmpl, bucket),  -- write subject prefix
		watcher = {},
	}, kvs_metatable)
	-- ADR-8/57: mirrors read from the mirror stream (same keys as the origin) and write to the origin
	if type(cfg.mirror) == 'table' and cfg.mirror.name then
		local origin = strip_kv(cfg.mirror.name)
		kv.pre = format(kvSubjectsPreTmpl, origin)
		kv.put_pre = kv.pre
		local ext = cfg.mirror.external
		if type(ext) == 'table' and ext.api and ext.api ~= '' then kv.put_pre = kv.pre end
	end
	return kv
end

local function bucket_of(cfg)
	if type(cfg) == 'string' then return { bucket = cfg } end
	if type(cfg) == 'table' and cfg.bucket then return cfg end
	return nil
end

-- client:kvs(bucket|cfg [, opts]) create or update (legacy entry point)
function cm.kvs(client, kvs_cfg, opts, action)
	kvs_cfg = bucket_of(kvs_cfg)
	if not kvs_cfg then return false, 'no name provided for kvs create' end
	opts = type(opts) == 'table' and opts or {}
	if not valid_bucket_name(kvs_cfg.bucket) then return false, 'bad bucket name' end
	local sc, err = stream_config(kvs_cfg)
	if not sc then return false, err end
	local stream, msg = cm.stream(client, sc, opts, action)
	if not stream then return nil, msg end
	return bind(stream, kvs_cfg.bucket), msg
end
function cm.create_key_value(client, cfg, opts) return cm.kvs(client, cfg, opts, 'create') end
function cm.update_key_value(client, cfg, opts) return cm.kvs(client, cfg, opts, 'update') end
function cm.create_or_update_key_value(client, cfg, opts) return cm.kvs(client, cfg, opts) end
-- bind to an existing bucket
function cm.key_value(client, bucket, opts)
	if not valid_bucket_name(bucket) then return nil, 'bad bucket name' end
	local stream, err = cm.stream(client, kv_stream_name(bucket), opts, 'info')
	if not stream then return nil, err == 'stream not found' and 'bucket not found' or err end
	return bind(stream, bucket)
end
function cm.delete_key_value(client, bucket, opts)
	if not valid_bucket_name(bucket) then return nil, 'bad bucket name' end
	return cm.delete_stream(client, kv_stream_name(bucket), opts)
end
function cm.key_value_store_names(client)
	local names, err = cm.stream_names(client, '$KV.*.>')
	if not names then return nil, err end
	local out = {}
	for _, n in ipairs(names) do if sub(n, 1, 3) == 'KV_' then out[#out + 1] = sub(n, 4) end end
	return out
end

-- ------------------------------------------------------------------ entries
local function op_of(h)
	local op = h[kvop]
	local reason = h['Nats-Marker-Reason'] -- ADR-48 limit markers
	if reason then
		if reason == 'Remove' then return kvdel end
		return kvpurge
	end
	return op or 'PUT'
end

local function iso_ns(ns)
	if not ns then return nil end
	return nats.timestamp(floor(ns / NS))
end

local function entry_from(kv, key, m, h, rev, created, delta, created_ns)
	local op = op_of(h)
	return {
		bucket = kv.name, key = key, value = (op == 'PUT') and m.data or nil,
		revision = rev, created = created, created_ns = created_ns, delta = delta, op = op,
	}
end
nats.kvs.entry_from = entry_from

-- get(key [, revision]) -> entry {bucket, key, value, revision, created, op} or nil, err
-- deleted/purged keys return nil, 'key not found', entry
function kvsm.get(kv, key, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local subject = kv.pre .. key
	local m, err = js.getmsg(kv.stream, rev, subject)
	if not m then
		if err == 'message not found' or (type(err) == 'table') then return nil, 'key not found' end
		return nil, err
	end
	if type(m.data) == 'table' then return nil, 'bucket without allow_direct is not supported' end
	local h = m:headers()
	if h['Nats-Subject'] and h['Nats-Subject'] ~= subject then return nil, 'key not found' end
	local e = entry_from(kv, key, m, h, tonumber(h['Nats-Sequence']), h['Nats-Time-Stamp'], 0)
	if e.op ~= 'PUT' then return nil, 'key not found', e end
	return e
end
function kvsm.get_revision(kv, key, rev) return kvsm.get(kv, key, rev) end

local function kv_publish(kv, key, value, header)
	local m, err = cm.get(unwrap(kv.stream.client), kv.put_pre .. key, value,
		header and next(header) and nats.encode_header(header) or nil)
	if not m then return nil, err end
	local ok, ack = pcall(decode, m.data)
	if ok and type(ack) == 'table' then
		if ack.error then return nil, ack.error.description or 'kv publish error', m, ack.error end
		rawset(m, 'ack', ack)
		rawset(m, 'revision', tonumber(ack.seq))
	end
	return m
end

-- put/update(key, value [, expected_revision]) -> reply msg (msg.revision, msg.ack) or nil, err
function kvsm.update(kv, key, value, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local header = rev and { ['Nats-Expected-Last-Subject-Sequence'] = tostring(floor(tonumber(rev))) } or nil
	return kv_publish(kv, key, value, header)
end
function kvsm.put(kv, key, value) return kvsm.update(kv, key, value, nil) end

-- create(key, value [, {ttl=seconds}]) succeeds only if the key does not exist (or is deleted)
function kvsm.create(kv, key, value, opts)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	opts = type(opts) == 'table' and opts or {}
	local header = { ['Nats-Expected-Last-Subject-Sequence'] = '0' }
	if opts.ttl then header['Nats-TTL'] = nats.jetstream.ttl_string(opts.ttl) end
	local m, err, raw, apierr = kv_publish(kv, key, value, header)
	if m then return m end
	if apierr and tonumber(apierr.err_code) == 10071 then -- wrong last sequence: deleted key?
		local _, gerr, e = kvsm.get(kv, key)
		if gerr == 'key not found' and e and e.op ~= 'PUT' then
			header['Nats-Expected-Last-Subject-Sequence'] = tostring(e.revision)
			return kv_publish(kv, key, value, header)
		end
		return nil, 'key exists', raw
	end
	return nil, err, raw
end

-- delete(key [, {revision=, purge=, ttl=}])
function kvsm.delete(kv, key, opts)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	opts = type(opts) == 'table' and opts or {}
	local header = {}
	if opts.purge then
		header[kvop] = kvpurge
		header['Nats-Rollup'] = 'sub'
		if opts.ttl then header['Nats-TTL'] = nats.jetstream.ttl_string(opts.ttl) end -- ADR-48
	else
		header[kvop] = kvdel
	end
	local rev = tonumber(opts.revision or opts.last_revision)
	if rev and rev > 0 then header['Nats-Expected-Last-Subject-Sequence'] = format('%d', rev) end
	return kv_publish(kv, key, nil, header)
end

function kvsm.purge(kv, key, opts)
	local o = {}
	for k, v in pairs(type(opts) == 'table' and opts or {}) do o[k] = v end
	o.purge = true
	return kvsm.delete(kv, key, o)
end

function kvsm.destroy(kv) return kv.stream:delete() end

function kvsm.status(kv)
	local stream, err = kv.stream:info()
	if not stream then return nil, err end
	kv.stream = stream
	local c = stream.config
	return {
		bucket = kv.name,
		values = stream.state.messages,
		bytes = stream.state.bytes,
		history = c.max_msgs_per_subject,
		ttl = (c.max_age or 0) / NS,
		limit_marker_ttl = (tonumber(c.subject_delete_marker_ttl) or 0) / NS,
		backing_store = 'JetStream',
		is_compressed = c.compression == 's2',
		metadata = c.metadata,
		stream = stream,
	}
end

-- ------------------------------------------------------------------ watchers
local Watcher = {}
Watcher.__index = Watcher
nats.kvs.watcher_metatable = Watcher

-- watch(keys, callback(entry|nil) [, opts]) -> watcher
--   keys: key, wildcard filter ('>' / 'a.*') or array of filters (ADR-34)
--   callback receives entries, and nil once at the end of the initial data
--   opts: include_history, ignore_deletes, meta_only, updates_only, resume_from_revision,
--         raw (legacy: plain core subscription with raw msgs, returns sid)
-- Updates are delivered while the client pumps (client:wait()).
function kvsm.watch(kv, keys, callback, opts)
	if type(keys) == 'function' then keys, callback, opts = '>', keys, callback end
	if type(callback) ~= 'function' then return nil, 'callback required' end
	opts = type(opts) == 'table' and opts or {}
	keys = keys or '>'
	if keys == '' then keys = '>' end
	local list = type(keys) == 'table' and keys or { keys }
	local filters = {}
	for i, k in ipairs(list) do
		if not valid_key_filter(k) then return nil, 'invalid key' end
		filters[i] = kv.pre .. k
	end
	if opts.raw then -- legacy behaviour: live updates only, raw messages, returns a sid
		return cm.subscribe(kv.stream.client, filters[1], callback)
	end
	local policy = 'last_per_subject'
	if opts.include_history then policy = 'all' end
	if opts.updates_only then policy = 'new' end
	local ocfg = { filter_subjects = filters, deliver_policy = policy, headers_only = opts.meta_only or nil }
	if opts.resume_from_revision then
		ocfg.deliver_policy = 'by_start_sequence'
		ocfg.opt_start_seq = floor(opts.resume_from_revision)
	end
	local w = setmetatable({ kv = kv, init_done = false, received = 0, callback = callback, opts = opts }, Watcher)
	local oc = js.ordered_consumer(kv.stream, ocfg)
	w.ordered = oc
	local pre_len = #kv.pre
	local function done()
		if not w.init_done then
			w.init_done = true
			pcall(callback, nil)
		end
	end
	local ctx, err = oc:consume(function(m)
		local meta = mm.metafields(m)
		local h = m:headers()
		local key = sub(m.subject, pre_len + 1)
		local e = entry_from(kv, key, m, h, meta.stream_seq, iso_ns(meta.timestamp), meta.pending, meta.timestamp)
		w.received = w.received + 1
		if not (opts.ignore_deletes and e.op ~= 'PUT') then
			local ok, cerr = pcall(callback, e)
			if not ok then w.last_error = cerr end
		end
		if not w.init_done and (meta.pending or 0) == 0 then done() end
	end, { max_messages = opts.max_messages or 256, idle_heartbeat = opts.idle_heartbeat, expires = opts.expires })
	if not ctx then return nil, err end
	w.ctx = ctx
	local c = oc.consumer
	if opts.updates_only or (c and (tonumber(c.num_pending) or 0) == 0 and (tonumber(c.delivered and c.delivered.consumer_seq) or 0) == 0) then
		done()
	end
	return w
end

function kvsm.watch_all(kv, callback, opts) return kvsm.watch(kv, '>', callback, opts) end

function Watcher:stop()
	if self.ctx then self.ctx.stop() self.ctx = nil end
end
Watcher.unsubscribe = Watcher.stop

-- pumps the client until the initial data was delivered (or timeout seconds)
function Watcher:wait_initial(timeout)
	local client = unwrap(self.kv.stream.client)
	local deadline = gettime() + (timeout or 10)
	while not self.init_done do
		local left = deadline - gettime()
		if left <= 0 then return false end
		cm.wait(client, { timeout = math.min(left, 0.5), maxcount = 1 })
	end
	return true
end

-- collect: runs a watcher until the end of the initial data
local function collect(kv, keys, opts, each, timeout)
	local w, err = kvsm.watch(kv, keys, function(e) if e then each(e) end end, opts)
	if not w then return nil, err end
	local ok = w:wait_initial(timeout)
	w:stop()
	if not ok then return nil, 'timeout' end
	return true
end

-- keys([filter(s)] [, opts]) -> array of keys (deleted keys excluded)
function kvsm.keys(kv, filters, opts)
	if type(filters) == 'table' and not filters[1] then opts, filters = filters, nil end
	opts = type(opts) == 'table' and opts or {}
	local out = {}
	local ok, err = collect(kv, filters or '>', { meta_only = true, ignore_deletes = true },
		function(e) out[#out + 1] = e.key end, opts.timeout)
	if not ok then return nil, err end
	return out
end
-- list_keys: iterator over keys
function kvsm.list_keys(kv, filters, opts)
	local keys, err = kvsm.keys(kv, filters, opts)
	if not keys then return nil, err end
	local i = 0
	return function() i = i + 1 return keys[i] end
end

-- history(key [, opts]) -> array of entries, oldest first
function kvsm.history(kv, key, opts)
	if not valid_key_filter(key) then return nil, 'invalid key' end
	opts = type(opts) == 'table' and opts or {}
	local out = {}
	local ok, err = collect(kv, key, { include_history = true, meta_only = opts.meta_only,
		ignore_deletes = opts.ignore_deletes }, function(e) out[#out + 1] = e end, opts.timeout)
	if not ok then return nil, err end
	if #out == 0 then return nil, 'key not found' end
	return out
end

-- purge_deletes([{older_than = seconds}]) removes delete/purge markers: markers older than
-- older_than (default 30 minutes, <= 0 means all) are removed completely, newer ones keep
-- only the marker itself.
function kvsm.purge_deletes(kv, opts)
	opts = type(opts) == 'table' and opts or {}
	local older = tonumber(opts.older_than) or DEFAULT_DELETE_MARKERS_OLDER_THAN
	local markers = {}
	local ok, err = collect(kv, '>', { meta_only = true }, function(e)
		if e.op ~= 'PUT' then markers[#markers + 1] = e end
	end, opts.timeout)
	if not ok then return nil, err end
	local now_ns = os.time() * NS
	local n = 0
	for _, e in ipairs(markers) do
		local keep = 1
		if older <= 0 or (e.created_ns and now_ns - e.created_ns > older * NS) then keep = 0 end
		local pok, perr = js.purge(kv.stream, nil, kv.pre .. e.key, keep > 0 and keep or nil)
		if not pok then return nil, perr end
		n = n + 1
	end
	return n
end

return nats
