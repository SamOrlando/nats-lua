-- nats-kvs.lua - JetStream Key-Value store (ADR-8) for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)

-- ### NATS-LUA Table
local nats = require('nats-jetstream')

-- ### Local properties ###
local encode, decode = nats.json.encode, nats.json.decode
local rawget, rawset = rawget, rawset
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber, tostring, pcall = tonumber, tostring, pcall
local type = type
local floor = math.floor
local match, format, find = string.match, string.format, string.find

local NS = nats.jetstream.NS

-- constants defined by nats go team and will continue to use
local kvBucketNameTmpl = "KV_%s"
local kvSubjectsTmpl = "$KV.%s.>"
local kvSubjectsPreTmpl = "$KV.%s."
local KeyValueMaxHistory = 64
local kvop = "KV-Operation"
local kvdel = "DEL"
local kvpurge = "PURGE"

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client meta methods

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

-- client:kvs(bucket|cfg [, opts])
-- cfg: bucket, description, history (1..64), ttl (seconds), max_bytes, max_value_size,
--      storage ('file'|'memory'), replicas, placement, republish, mirror, sources, compression
function cm.kvs(client, kvs_cfg, opts)
	if type(kvs_cfg) == "table" and kvs_cfg.bucket then -- luacheck: ignore 542
	elseif type(kvs_cfg) == "string" then kvs_cfg = { bucket = kvs_cfg }
	else return false, 'no name provided for kvs create' end
	opts = type(opts) == 'table' and opts or {}

	if not valid_bucket_name(kvs_cfg.bucket) then return false, 'bad bucket name' end

	local ttl = tonumber(kvs_cfg.ttl)
	local history = tonumber(kvs_cfg.history) or 1
	if history < 1 or history > KeyValueMaxHistory then return false, 'history must be between 1 and 64' end
	local dup = 2 * 60 * NS
	if ttl and ttl > 0 and ttl * NS < dup then dup = floor(ttl * NS) end

	local stream_cfg = { -- stream config (ADR-8)
		name = format(kvBucketNameTmpl, kvs_cfg.bucket),
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
		duplicate_window = dup,
		allow_rollup_hdrs = true,
		deny_delete = true,
		deny_purge = false,
		allow_direct = true,
		retention = 'limits',
		compression = kvs_cfg.compression,
		placement = kvs_cfg.placement,
		republish = kvs_cfg.republish,
		mirror = kvs_cfg.mirror,
		mirror_direct = kvs_cfg.mirror and true or nil,
		sources = kvs_cfg.sources,
	}

	local stream, msg = client:stream(stream_cfg, opts)
	if not stream then return nil, msg end

	return setmetatable({
		name = kvs_cfg.bucket,
		pre = format(kvSubjectsPreTmpl, kvs_cfg.bucket),
		stream = stream,
		watcher = {},
	}, kvs_metatable), msg
end

-- get(key [, revision]) -> entry {bucket, key, value, revision, created, op} or nil, err
function kvsm.get(kvs, key, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local m, err = kvs.stream:getmsg(rev, kvs.pre .. key)
	if not m then
		if err == 'message not found' then return nil, 'key not found' end
		return nil, err
	end
	local header = m:headers()
	if rev and header['Nats-Subject'] and header['Nats-Subject'] ~= kvs.pre .. key then
		return nil, 'key not found'
	end
	local op = header[kvop]
	local value = m.data
	if op == kvpurge or op == kvdel then value = nil end
	return {
		bucket = kvs.name,
		key = key,
		value = value,
		created = header['Nats-Time-Stamp'],
		revision = tonumber(header['Nats-Sequence']),
		delta = nil,
		op = op or 'PUT',
	}
end

-- put/update(key, value [, expected_revision]) -> reply msg (msg.ack = pub ack) or nil, err
function kvsm.update(kvs, key, value, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local header = rev and nats.encode_header({ ['Nats-Expected-Last-Subject-Sequence'] = tostring(tonumber(rev)) })
	local m, err = cm.get(kvs.stream.client, kvs.pre .. key, value, header)
	if not m then return nil, err end
	local ok, ack = pcall(decode, m.data)
	if ok and type(ack) == 'table' then
		if ack.error then return nil, ack.error.description or 'kv put error', m end
		rawset(m, 'ack', ack)
		rawset(m, 'revision', ack.seq)
	end
	return m
end
kvsm.put = kvsm.update

function kvsm.create(kvs, key, value) return kvsm.update(kvs, key, value, 0) end

function kvsm.delete(kvs, key, opts) -- this is not to delete the kvs, that would be destroy.
	if not valid_key_name(key) then return nil, 'invalid key name' end
	opts = type(opts) == 'table' and opts or {}
	local header = {}
	if opts.purge then
		header[kvop] = kvpurge
		header['Nats-Rollup'] = 'sub'
	else
		header[kvop] = kvdel
	end
	if opts.revision and opts.revision > 0 then
		header['Nats-Expected-Last-Subject-Sequence'] = tostring(tonumber(opts.revision))
	end
	local m, err = cm.get(kvs.stream.client, kvs.pre .. key, nil, nats.encode_header(header))
	if not m then return nil, err end
	local ok, ack = pcall(decode, m.data)
	if ok and type(ack) == 'table' and ack.error then return nil, ack.error.description or 'kv delete error', m end
	return m
end

function kvsm.purge(kvs, key, opts)
	opts = type(opts) == 'table' and opts or {}
	opts.purge = true
	return kvsm.delete(kvs, key, opts)
end

function kvsm.destroy(kvs) return kvs.stream:delete() end

function kvsm.status(kvs)
	local stream, err = kvs.stream:info()
	if not stream then return nil, err end
	return {
		bucket = kvs.name,
		values = stream.state.messages,
		bytes = stream.state.bytes,
		history = stream.config.max_msgs_per_subject,
		ttl = (stream.config.max_age or 0) / NS,
		backing_store = 'JetStream',
		stream = stream,
	}
end

function kvsm.purge_deletes() return nil, 'not implemented' end

-- live updates only (no initial values): core subscription on the key subject
function kvsm.watch(kvs, key, func)
	if type(func) ~= 'function' then return nil, 'callback required' end
	if key ~= '>' and key ~= '*' and not nats.valid_subject(kvs.pre .. key) then return nil, 'invalid key' end
	return cm.subscribe(kvs.stream.client, kvs.pre .. key, func)
end

function kvsm.watch_all(kvs, func)
	if type(func) ~= 'function' then return nil, 'callback required' end
	return cm.subscribe(kvs.stream.client, kvs.pre .. '>', func)
end

function kvsm.keys() return nil, 'not implemented' end
function kvsm.list_keys() return nil, 'not implemented' end
function kvsm.history() return nil, 'not implemented' end
return nats
