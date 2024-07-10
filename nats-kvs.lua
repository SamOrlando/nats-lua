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
		--consumer_limits = nil,
		deny_delete = true,
		--deny_purge = nil,
		description = kvs_cfg.description,
		--discard = nil,
		--discard_new_per_subject = nil,
		duplicate_window = (kvs_cfg.ttl and ((kvs_cfg.ttl > 0) and (kvs_cfg.ttl < 2000))) and kvs_cfg.ttl,
		--first_seq = nil,
		max_age = kvs_cfg.ttl,
		max_bytes = (kvs_cfg.max_bytes and (kvs_cfg.max_bytes > 0)) and kvs_cfg.max_bytes or -1,
		max_consumers = -1,
		max_msg_size = (kvs_cfg.max_value_size and (kvs_cfg.max_value_size > 0)) and max_value_size or -1,
		max_msgs = -1,
		max_msgs_per_subject = (kvs_cfg.history and ((kvs_cfg.history > 0) and (kvs_cfg.history <= KeyValueMaxHistory))) and kvs_cfg.history or 1,
		--metadata = nil,
		mirror = kvs_cfg.mirror,
		mirror_direct = kvs_cfg.mirror and true,
		name = format(kvBucketNameTmpl, kvs_cfg.bucket),
		--no_ack = nil,
		num_replicas = (kvs_cfg.replicas and (kvs_cfg.replicas > 0)) and kvs_cfg.replicas or 1,
		placement = kvs_cfg.placement,
		republish = kvs_cfg.republish,
		retention = kvs_cfg.retention,
		--sealed = nil,
		sources = kvs_cfg.sources, -- not sure what to do here but its not simple...
		storage = kvs_cfg.storage,
		subject_transform = nil, -- ss.SubjectTransforms = []SubjectTransformConfig{{Source: fmt.Sprintf(kvSubjectsTmpl, sourceBucketName), Destination: fmt.Sprintf(kvSubjectsTmpl, cfg.Bucket)}}
		subjects = {format(kvSubjectsTmpl, kvs_cfg.bucket)}, -- related to subjects from bucket/stream name.
		--template_owner = nil,
	}
	
	local stream, msg = client:stream(stream_cfg, opts)
	if not stream then return false, msg end
	
	-- create and set stream object
	local kvs = setmetatable({
		js = stream, -- jetstream ??? no idea what they want here
		name = kvs_cfg.bucket,
		pre = format(kvSubjectsPreTmpl, kvs_cfg.bucket),
		putPre = nil,
		stream = stream, -- strem obj
	}, kvs_metatable)
	
	kvs_table[kvs.name] = kvs

	return kvs, msg
end

-- Adding KVS Interface Methods
local kvsm = nats.kvs.metatable.methods
function kvsm:get(key, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local s = self.pre .. key .. '.' .. (rev and rev or '0')
	return self.stream.client:get(s)
	-- setup string with pre and stuff for subject
	-- use direct check
	-- if not rev, get last msg of topic, if rev then get that.
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

    -- return entry, nil
-- end

function kvsm:put(key, value, rev)
	if not valid_key_name(key) then return nil, 'invalid key name' end
	local s = self.pre .. key .. (rev and '.' or '') .. (rev and rev or '')
	self.stream.client:publish(s, value)
	-- validate key name
	-- check if we use JSPfx
	-- check if putPre or pre (wtf..)
	-- revision?
	-- publish to correct subject
end

-- function kvsm:put(key, value)
    -- if not keyValid(key) then return nil, 'invalid key name' end

    -- local s = ''
    -- if self.useJSPfx then s = s .. self.js.opts.pre end
    -- if self.putPre then s = s .. self.putPre
    -- elseif self.pre then s = s .. self.pre end
    
	-- s = s .. key

    -- local pa, err = self.js:Publish(b, value)
    -- if err then
        -- return 0, err
    -- end
    -- return pa.Sequence, err
-- end


function kvsm:create(key, value, rev) end
function kvsm:update(key, value, rev) end
function kvsm:delete(key, opts) end
function kvsm:purge_deletes(opts) end
function kvsm:purge(key, opts) end
function kvsm:watch(key, opts) end
function kvsm:watchall(opts) end
function kvsm:keys(opts) end
function kvsm:list_keys(opts) end
function kvsm:history(key, opts) end
function kvsm:bucket() end
function kvsm:bucket_delete() end
function kvsm:status() end

return nats




-- main part of module
-- // KeyValueManager is used to manage KeyValue stores. It provides methods to
-- KeyValue(ctx context.Context, bucket string) (KeyValue, error)
-- CreateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
-- UpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
-- CreateOrUpdateKeyValue(ctx context.Context, cfg KeyValueConfig) (KeyValue, error)
-- DeleteKeyValue(ctx context.Context, bucket string) error
-- KeyValueStoreNames(ctx context.Context) KeyValueNamesLister
-- KeyValueStores(ctx context.Context) KeyValueLister


-- streams = {
-- host-events = {
  -- sealed = false
  -- deny_delete = false
  -- deny_purge = false
  -- allow_rollup_hdrs = false
  -- consumer_limits = {
  -- }
  -- compression = none
  -- retention = limits
  -- max_consumers = -1
  -- max_msgs = -1
  -- max_bytes = -1
  -- subjects = {
	-- 1 = host.event.>
  -- }
  -- max_msgs_per_subject = -1
  -- max_msg_size = -1
  -- discard = old
  -- name = host-events
  -- storage = file
  -- max_age = 0
  -- num_replicas = 1
  -- duplicate_window = 120000000000
  -- consumers = {
	-- woots = {
	  -- deliver_policy = all
	  -- ack_policy = none
	  -- max_deliver = -1
	  -- replay_policy = instant
	  -- num_replicas = 0
	  -- name = woots
	  -- inactive_threshold = 5000000000
	  -- max_waiting = 512
	-- }
  -- }
  -- allow_direct = false
  -- mirror_direct = false
-- }




	-- // KeyValueConfig is the configuration for a KeyValue store.
	-- KeyValueConfig struct {
		-- // Bucket is the name of the KeyValue store. Bucket name has to be
		-- // unique and can only contain alphanumeric characters, dashes, and
		-- // underscores.
		-- Bucket string

		-- // Description is an optional description for the KeyValue store.
		-- Description string

		-- // MaxValueSize is the maximum size of a value in bytes. If not
		-- // specified, the default is -1 (unlimited).
		-- MaxValueSize int32

		-- // History is the number of historical values to keep per key. If not
		-- // specified, the default is 1. Max is 64.
		-- History uint8

		-- // TTL is the expiry time for keys. By default, keys do not expire.
		-- TTL time.Duration

		-- // MaxBytes is the maximum size in bytes of the KeyValue store. If not
		-- // specified, the default is -1 (unlimited).
		-- MaxBytes int64

		-- // Storage is the type of storage to use for the KeyValue store. If not
		-- // specified, the default is FileStorage.
		-- Storage StorageType

		-- // Replicas is the number of replicas to keep for the KeyValue store in
		-- // clustered jetstream. Defaults to 1, maximum is 5.
		-- Replicas int

		-- // Placement is used to declare where the stream should be placed via
		-- // tags and/or an explicit cluster name.
		-- Placement *Placement

		-- // RePublish allows immediate republishing a message to the configured
		-- // subject after it's stored.
		-- RePublish *RePublish

		-- // Mirror defines the consiguration for mirroring another KeyValue
		-- // store.
		-- Mirror *StreamSource

		-- // Sources defines the configuration for sources of a KeyValue store.
		-- Sources []*StreamSource

		-- // Compression sets the underlying stream compression.
		-- // NOTE: Compression is supported for nats-server 2.10.0+
		-- Compression bool
	-- }

	-- // KeyLister is used to retrieve a list of key value store keys. It returns
	-- // a channel to read the keys from. The lister will always close the channel
	-- // when done (either all keys have been read or an error occurred) and
	-- // therefore can be used in range loops. Stop can be used to stop the lister
	-- // when not all keys have been read.
	-- KeyLister interface {
		-- Keys() <-chan string
		-- Stop() error
	-- }

	-- // KeyValueLister is used to retrieve a list of key value stores. It returns
	-- // a channel to read the KV store statuses from. The lister will always
	-- // close the channel when done (either all stores have been retrieved or an
	-- // error occurred) and therefore can be used in range loops. Stop can be
	-- // used to stop the lister when not all KeyValue stores have been read.
	-- KeyValueLister interface {
		-- Status() <-chan KeyValueStatus
		-- Error() error
	-- }

	-- // KeyValueNamesLister is used to retrieve a list of key value store names.
	-- // It returns a channel to read the KV bucket names from. The lister will
	-- // always close the channel when done (either all stores have been retrieved
	-- // or an error occurred) and therefore can be used in range loops. Stop can
	-- // be used to stop the lister when not all bucket names have been read.
	-- KeyValueNamesLister interface {
		-- Name() <-chan string
		-- Error() error
	-- }

	-- // KeyValueStatus is run-time status about a Key-Value bucket.
	-- KeyValueStatus interface {
		-- // Bucket returns the name of the KeyValue store.
		-- Bucket() string

		-- // Values is how many messages are in the bucket, including historical values.
		-- Values() uint64

		-- // History returns the configured history kept per key.
		-- History() int64

		-- // TTL returns the duration for which keys are kept in the bucket.
		-- TTL() time.Duration

		-- // BackingStore indicates what technology is used for storage of the bucket.
		-- // Currently only JetStream is supported.
		-- BackingStore() string

		-- // Bytes returns the size of the bucket in bytes.
		-- Bytes() uint64

		-- // IsCompressed indicates if the data is compressed on disk.
		-- IsCompressed() bool
	-- }

	-- // KeyWatcher is what is returned when doing a watch. It can be used to
	-- // retrieve updates to keys. If not using UpdatesOnly option, it will also
	-- // send the latest value for each key. After all initial values have been
	-- // sent, a nil entry will be sent. Stop can be used to stop the watcher and
	-- // close the underlying channel. Watcher will not close the channel until
	-- // Stop is called or connection is closed.
	-- KeyWatcher interface {
		-- Updates() <-chan KeyValueEntry
		-- Stop() error
	-- }

	-- // KeyValueEntry is a retrieved entry for Get, List or Watch.
	-- KeyValueEntry interface {
		-- // Bucket is the bucket the data was loaded from.
		-- Bucket() string

		-- // Key is the name of the key that was retrieved.
		-- Key() string

		-- // Value is the retrieved value.
		-- Value() []byte

		-- // Revision is a unique sequence for this value.
		-- Revision() uint64

		-- // Created is the time the data was put in the bucket.
		-- Created() time.Time

		-- // Delta is distance from the latest value (how far the current sequence
		-- // is from the latest).
		-- Delta() uint64

		-- // Operation returns Put or Delete or Purge, depending on the manner in
		-- // which the current revision was created.
		-- Operation() KeyValueOp
	-- }
-- )

-- type (
	-- WatchOpt interface {
		-- configureWatcher(opts *watchOpts) error
	-- }

	-- watchOpts struct {
		-- // Do not send delete markers to the update channel.
		-- ignoreDeletes bool
		-- // Include all history per subject, not just last one.
		-- includeHistory bool
		-- // Include only updates for keys.
		-- updatesOnly bool
		-- // retrieve only the meta data of the entry
		-- metaOnly bool
		-- // resumeFromRevision is the revision to resume from.
		-- resumeFromRevision uint64
	-- }

	-- // KVDeleteOpt is used to configure delete and purge operations.
	-- KVDeleteOpt interface {
		-- configureDelete(opts *deleteOpts) error
	-- }

	-- deleteOpts struct {
		-- // Remove all previous revisions.
		-- purge bool

		-- // Delete only if the latest revision matches.
		-- revision uint64
	-- }

	-- // KVPurgeOpt is used to configure PurgeDeletes.
	-- KVPurgeOpt interface {
		-- configurePurge(opts *purgeOpts) error
	-- }

	-- purgeOpts struct {
		-- dmthr time.Duration // Delete markers threshold
	-- }
-- )

-- // KeyValueOp represents the type of KV operation (Put, Delete, Purge). It is a
-- // part of KeyValueEntry.
-- type KeyValueOp uint8

-- // Available KeyValueOp values.
-- const (
	-- // KeyValuePut is a set on a revision which creates or updates a value for a
	-- // key.
	-- KeyValuePut KeyValueOp = iota

	-- // KeyValueDelete is a set on a revision which adds a delete marker for a
	-- // key.
	-- KeyValueDelete

	-- // KeyValuePurge is a set on a revision which removes all previous revisions
	-- // for a key.
	-- KeyValuePurge
-- )

-- func (op KeyValueOp) String() string {
	-- switch op {
	-- case KeyValuePut:
		-- return "KeyValuePutOp"
	-- case KeyValueDelete:
		-- return "KeyValueDeleteOp"
	-- case KeyValuePurge:
		-- return "KeyValuePurgeOp"
	-- default:
		-- return "Unknown Operation"
	-- }
-- }

-- // Regex for valid keys and buckets.
-- var (
	-- validBucketRe = regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	-- validKeyRe    = regexp.MustCompile(`\A[-/_=\.a-zA-Z0-9]+\z`)
-- )
