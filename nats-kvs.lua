-- ### NATS-LUA Table
local nats = require('nats-jetstream')

-- ### Library requirements ###
local cjson = require('cjson')
local nuid = require('nuid')
local gettime = require('socket').gettime

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode
local json_error_true = '{\"error\":true}'
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local match, format = string.match, string.format
local insert = table.insert

-- constants defined by nats go team and will continue to use
local kvBucketNamePre = "KV_"
local kvBucketNameTmpl = "KV_%s"
local kvSubjectsTmpl = "$KV.%s.>"
local kvSubjectsPreTmpl = "$KV.%s."
local kvSubjectsPreDomainTmpl = "%s.$KV.%s."
local kvNoPending = "0"
local KeyValueMaxHistory = 64
local AllKeys = ">"
local kvLatestRevision = 0
local kvop = "KV-Operation"
local kvdel = "DEL"
local kvpurge = "PURGE"

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client meta methods
local js = nats.jetstream.stream_metatable.methods -- stream meta methods
local con = nats.jetstream.consumer_metatable.methods -- consumer meta methods
local mm = nats.msg_metatable.methods -- msg meta methods

-- kvs is the implementation of KeyValueStore
nats.kvs = {
	metatable = {
		__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
		--__newindex = function(self, key, value) print("__newindex kvs: " .. key .. " with a value of " .. tostring(value)) end,
		methods = {},
	}
}
local kvs_metatable = nats.kvs.metatable
local kvsm = nats.kvs.metatable.methods -- kvs methods table for easy ref(lol)

--local kvs_table = {} -- table of all the keyvalue stores by name

function nats.valid_bucket_name(s) return match(s, "^[%w%-_]+$") end
local valid_bucket_name = nats.valid_bucket_name
function nats.valid_key_name(s) return match(s, "^[-/_=.%a%d]+$") end
local valid_key_name = nats.valid_key_name

function cm.kvs(client, kvs_cfg, opts) --kvs:(string or obj)
	-- test args and clear if needed
	if type(kvs_cfg) == "table" and kvs_cfg.bucket then
	elseif type(kvs_cfg) == "string" then kvs_cfg = {bucket=kvs_cfg}
	else return false, 'no name provided for stream create' end
	opts = type(opts) == 'table' and opts or {}
	
	if not valid_bucket_name(kvs_cfg.bucket) then return false, 'bad bucket name' end
	
	local stream_cfg = { -- stream config
		allow_direct = true,
		allow_rollup_hdrs = true,
		compression = kvs_cfg.compression,
		deny_delete = true,
		description = kvs_cfg.description,
		duplicate_window = (kvs_cfg.ttl and ((kvs_cfg.ttl > 0) and (kvs_cfg.ttl < 2000))) and kvs_cfg.ttl,
		max_age = kvs_cfg.ttl,
		max_bytes = (kvs_cfg.max_bytes and (kvs_cfg.max_bytes > 0)) and kvs_cfg.max_bytes or -1,
		max_consumers = -1,
		max_msg_size = (kvs_cfg.max_value_size and (kvs_cfg.max_value_size > 0)) and max_value_size or -1,
		max_msgs = -1,
		max_msgs_per_subject = (kvs_cfg.history and ((kvs_cfg.history > 0) and (kvs_cfg.history <= KeyValueMaxHistory))) and kvs_cfg.history or 1,
		mirror = kvs_cfg.mirror,
		mirror_direct = kvs_cfg.mirror and true,
		name = format(kvBucketNameTmpl, kvs_cfg.bucket),
		num_replicas = (kvs_cfg.replicas and (kvs_cfg.replicas > 0)) and kvs_cfg.replicas or 1,
		placement = kvs_cfg.placement,
		republish = kvs_cfg.republish,
		retention = kvs_cfg.retention,
		sources = kvs_cfg.sources, -- not sure what to do here but its not simple...
		storage = kvs_cfg.storage,
		subject_transform = nil, -- ss.SubjectTransforms = []SubjectTransformConfig{{Source: fmt.Sprintf(kvSubjectsTmpl, sourceBucketName), Destination: fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}}
		subjects = {format(kvSubjectsTmpl, kvs_cfg.bucket)}, -- related to subjects from bucket/stream name.
	}
	
	local stream, msg = client:stream(stream_cfg, opts)
	if not stream then return nil, msg end
	
	-- create and set stream object
	return setmetatable({
		name = kvs_cfg.bucket,
		pre = format(kvSubjectsPreTmpl, kvs_cfg.bucket),
		--putPre = nil,
		stream = stream, -- strem obj
		watcher = {}, -- watcher table
	}, kvs_metatable), msg
end

-- KVS Methods
local kvsm = nats.kvs.metatable.methods
function kvsm.get(kvs, key, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local m = kvs.stream:getmsg(rev, kvs.pre .. key) -- msg
	if m then
		local header = m.header and nats.decode_header(m.header) or {}
		local op = header['KV-Operation']
		if op and (op == 'PURGE' or op == 'DEL') then m.data = nil end
		return {
			bucket = kvs.name,
			key = key,
			value = m.data,
			created = header['Nats-Time-Stamp'], -- or nats.timestamp(),
			revision = tonumber(header['Nats-Sequence']), --  or rev,
			delta = nil,
			op = op or 'PUT',
		}
	end
end

function kvsm.update(kvs, key, value, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local header = rev and nats.encode_header({['Nats-Expected-Last-Subject-Sequence'] = tonumber(rev)})
	return kvs.stream.client:get(kvs.pre .. key, value, header)
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
		header['Nats-Expected-Last-Subject-Sequence'] = tonumber(opts.revision)
	end
	return kvs.stream.client:get(kvs.pre .. key, nil, nats.encode_header(header))
end

function kvsm.purge(kvs, key, opts)
	opts = type(opts) == 'table' and opts or {}
	opts.purge = true
	return kvsm.delete(kvs, key, opts)
end

function kvsm.destroy(kvs) return kvs.stream:delete() end -- only way to keep function within object and not overlap delete function.

function kvsm.purge_deletes(kvs, opts) end
function kvsm.status(kvs) end

function kvsm.watch(kvs, key, func, opts) -- TODO provide more inclusive wrapper handling opts
	if not valid_key_name(key) or type(func) ~= 'function' then return end
	--opts = type(opts) == 'table' and opts or {}
	return cm.subscribe(kvs.stream.client, kvs.pre .. key, func)
end

function kvsm.watch_all(kvs, func, opts) -- TODO provide more inclusive wrapper handling opts
	if type(func) ~= 'function' then return end
	return cm.subscribe(kvs.stream.client, kvs.pre .. '*', func) -- TODO > or *?
end

function kvsm.keys(kvs, opts) end
function kvsm.list_keys(kvs, opts) end
function kvsm.history(kvs, key, opts) end
return nats
