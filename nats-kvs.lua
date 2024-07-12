-- TODO: Handling of MIRROR, SOURCES, Externals, API Prefix and wtfs

-- ### NATS-LUA Table
local nats = require('nats-jetstream')

-- ### Library requirements ###
local cjson = require('cjson')
local nuid = require('nuid')

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode
local json_error_true = '{\"error\":true}'
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local pairs, ipairs, type = pairs, ipairs, type
local match, format = string.match, string.format

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

-- kve is the implementation of KeyValueEntry
nats.kve = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
--	__newindex = function(self, key, value) print("__newindex kvs: " .. key .. " with a value of " .. tostring(value)) end,
	metatable = {
		methods = {},
		}
}
local kve_metatable = nats.kve.metatable
local kvem = nats.kve.metatable.methods -- kve methods table for easy ref

function cm.kve(t)
	if type(t) ~= "table" then t = {} end
	return setmetatable({
		bucket = t.bucket,
		key = t.key,
		value = t.value,
		revision = t.revision,
		delta = t.delta,
		create = gettime(),
		op = t.op
	}, nats.kve.metatable)
end

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

local kvs_table = {} -- table of all the keyvalue stores by name

local function valid_bucket_name(s) return match(s, "^[%w%-_]+$") end
local function valid_key_name(s) return match(s, "^[-/_=.%a%d]+$") end

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
	if not stream then return false, msg end
	
	-- create and set stream object
	local kvs = setmetatable({
		name = kvs_cfg.bucket,
		pre = format(kvSubjectsPreTmpl, kvs_cfg.bucket),
		--putPre = nil,
		stream = stream, -- strem obj
	}, kvs_metatable)
	
	kvs_table[kvs.name] = kvs

	return kvs, msg
end

-- Adding KVS Interface Methods
local kvsm = nats.kvs.metatable.methods
function kvsm:get(key, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local s = self.pre .. key
	return self.stream:getmsg(rev, s)
	-- get result and create kve
	-- wtf is this? // Double check here that this is not a DEL Operation marker.
	-- return kve
end
    -- local entry = {
        -- bucket = kv.name,
        -- key = key,
        -- value = m.Data,
        -- revision = m.Sequence,
        -- created = m.Time,
    -- }
    -- -- Double check here that this is not a DEL Operation marker.
    -- if #m.Header > 0 then
        -- local kvop_value = m.Header:Get(kvop)
        -- if kvop_value == kvdel then
            -- entry.op = KeyValueDelete
            -- return entry, ErrKeyDeleted
        -- elseif kvop_value == kvpurge then
            -- entry.op = KeyValuePurge
            -- return entry, ErrKeyDeleted
        -- end
    -- end

function kvsm:update(key, value, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local s = self.pre .. key
	if rev then
		return self.stream.client:get(s, value, nats.encode_header({['Nats-Expected-Last-Subject-Sequence'] = tonumber(rev)}))
	else
		return self.stream.client:get(s, value)
	end
end
kvsm.put = kvsm.update

function kvsm:create(key, value) return self:update(key, value, 0) end

function kvsm:delete(key, opts)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	opts = type(opts) == 'table' and opts or {}
	local header = {}
	local s = self.pre .. key
	if opts.purge then
		header['KV-Operation'] = 'PURGE'
		header['Nats-Rollup'] = 'sub'
	else
		header['KV-Operation'] = 'DEL'
	end
	if opts.revision and opts.revision > 0 then 
		header['Nats-Expected-Last-Subject-Sequence'] = tonumber(opts.revision)
	end
	return self.stream.client:get(s, nil, nats.encode_header(header))
end

function kvsm:purge(key, opts)
	opts = type(opts) == 'table' and opts or {}
	opts.purge = true
	return self:delete(key, opts)
end

function kvsm:purge_deletes(opts) end

function kvsm:watch(key, opts) end
function kvsm:watchall(opts) end
function kvsm:keys(opts) end
function kvsm:list_keys(opts) end
function kvsm:history(key, opts) end

return nats