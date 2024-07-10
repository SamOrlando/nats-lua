-- ### NATS-LUA Table
local nats = require('nats')
nats.jetstream = {}

-- ### Library requirements ###
local cjson = require('cjson')
local nuid = require('nuid')

-- ### Local properties ### speed and neet laziness ftw
local encode,decode = cjson.encode, cjson.decode
local json_error_true = '{\"error\":true}'
local rawset, rawget = rawset, rawget
local getmetatable, setmetatable = getmetatable, setmetatable
local tonumber = tonumber
local format = string.format

local defaultAPIPrefix = "$JS.API."
local jsDomainT = "$JS.%s.API."
local jsExtDomainT = "$JS.%s.API"
local jsAPIPrefix = "%s."

local stream_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex stream: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
}
nats.jetstream.stream_metatable = stream_metatable

local consumer_metatable = {
	__index = function(self, key) return rawget(getmetatable(self), "methods")[key] or rawget(self, key) end,
	__newindex = function(self, key, value) print("__newindex consumer: " .. key .. " with a value of " .. tostring(value)) end,
	methods = {},
}
nats.jetstream.consumer_metatable = consumer_metatable

-- quick local references to method tables for each object
local cm = nats.client_metatable.methods -- client meta methods
local js = stream_metatable.methods -- stream meta methods
local con = consumer_metatable.methods -- consumer meta methods
local mm = nats.msg_metatable.methods -- msg meta methods 

function cm.stream(client, scfg, opts)
	-- test args and clear if needed
	opts = opts or {}
	if type(scfg) == 'string' then scfg = {name=scfg}
	elseif type(scfg) == 'table' and scfg.name then
	else return false, 'no stream name provided' end

	local nc = client.client or client -- encoded connections and feels bad

	local stream_api = opts.domain and format(jsDomainT, opts.domain)
		or opts.pre	or defaultAPIPrefix

	local msg = nc:get(stream_api .. 'STREAM.CREATE.' .. scfg.name, encode(scfg))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	local data = msg.data
	local stream = setmetatable({
		api = stream_api,
		name = data.config.name,
		client = nc, -- we forcing base client as we are doing our own encoding with-in jetstream module
		consumers = {},
		state = data.state,
		config = data.config,
		cluster = data.cluster,
		created = data.created,
		ts = data.ts,
		opts = { -- this is sloppy alignment
			pre = opts.pre and format(jsAPIPrefix, opts.pre),
			maxpa = opts.maxpa or 4000, -- stupid var whats it for?
			wait = opts.wait,
			aecb = nil,
			domain = opts.domain,
			purge_opts = nil, -- table,
			stream_list_subject = nil, --?
			direct_get = nil,
			feature_flags = nil, -- table
		},
	}, stream_metatable)
	
	-- TODO: MIRROR CHECK AND CONFIGURATION
	-- TODO: SOURCE CHECK AND CONFIGURATION
	
	-- insert stream info into client table
	if not rawget(nc, "streams") then rawset(nc, "streams", {}) end
	nc.streams[stream.config.name] = stream
	
	return stream, msg
end

function js.delete(stream)
	-- make request
	local msg = stream.client:get(stream.api .. 'STREAM.DELETE.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	
	-- delete client objects stream listing and the object
	stream.client.streams[stream.name] = nil
	stream = nil -- i know...
	return true, msg
end

function js.consumer(stream, consumer_name, config)
	local msg = stream.client:get(stream.api .. 'CONSUMER.CREATE.' .. stream.name .. '.' .. consumer_name, encode(config or {stream_name=stream.name,name=consumer_name}))
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end

	-- consumers assigned subscription
	local sid = cm.subscribe(stream.client, "_INBOX." .. stream.name .. "." .. consumer_name, function(m)
		print("got stream msg")
		return m
	end)

	-- create and set consumer object
	local consumer = setmetatable({
		name = consumer_name,
		stream = stream,
		config = msg.data.config,
		cluster = msg.data.cluster,
		sid = sid,
	}, consumer_metatable)

	-- insert consumer info into stream table and client 
	stream.consumers[consumer_name] = consumer.config
	stream.client.streams[stream.name].consumers = stream.client.streams[stream.name].consumers or {}
	stream.client.streams[stream.name].consumers[consumer_name] = consumer.config
	return consumer, msg
end

function con.delete(consumer)
	local stream = consumer.stream
	local msg = stream.client:get(stream.api .. 'CONSUMER.DELETE.' .. stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	-- remove assigned sub
	cm.unsubscribe(stream.client, consumer.sid)
	-- delete references
	stream.consumers[consumer.name] = nil
	stream.client.streams[stream.name].consumers[consumer.name] = nil
	return true, msg
end

function js.publish(stream, subject, data, reply, header)
	local msg = stream.client:publish(subject, data, reply, header)
	return true, msg
end

function js.info(stream)
	local msg = stream.client:get(stream.api .. 'STREAM.INFO.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function con.info(consumer)
	local msg = consumer.stream.client:get(consumer.stream.api .. 'CONSUMER.INFO.' .. consumer.stream.name .. '.' .. consumer.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function con.nextmsg(consumer)
	local msg = consumer.stream.client:get(consumer.stream.api .. 'CONSUMER.MSG.NEXT.' .. consumer.stream.name .. '.' .. consumer.name, nil, nil, consumer.sid)
	if msg then
		msg.data = decode(msg.data or json_error_true)
		if msg.data.error then return false, msg end
		return msg
	else return end
end

function cm.stream_list(client)
	local msg = client:get("$JS.API.STREAM.LIST")
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.consumer_list(stream)
	local msg = stream.client:get(stream.api .. 'CONSUMER.LIST.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg.data
end

function js.update(stream, configuration)
	local configJson = encode(configuration or {})
	local msg = stream.client:get(stream.api .. 'STREAM.UPDATE.' .. stream.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function con.update(consumer, configuration)
	local configJson = encode(configuration)
	local msg = consumer.client:get(consumer.stream.api .. 'CONSUMER.UPDATE.' .. consumer.stream.name .. '.' .. consumer.name, configJson)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.replay(stream, msg_sequence)
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.GET.' .. stream.name, msg_sequence)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return msg
end

function js.remove(stream, msg_sequence) -- msg:remove or delete()? conflict need to resolve in msg and own tables
	local msg = stream.client:get(stream.api .. 'STREAM.MSG.DELETE.' .. stream.name, msg_sequence)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

function js.purge(stream)
	local msg = stream.client:get(stream.api .. 'STREAM.PURGE.' .. stream.name)
	msg.data = decode(msg.data or json_error_true)
	if msg.data.error then return false, msg end
	return true, msg
end

-- modify and make msg objects jetstream enabled
local function ack_msg(m, t, d, ack) -- msg, type, delay, wait for ack?
	if m.ackd then return false, "Message already acknolwedged." end
	if t and t ~= "+WIP" then rawset(m, "ackd", true) end
	if d then t = (t or "-NAK") .. " {\"delay\": " .. ((tonumber(d) or 0)*1000) .. "}" end
	if t == "+NXT" or (ack and m.reply) then return m.new():get(m.reply, t) -- unsure what to do here...
	else return m:respond(t) end
end
function mm.is_ackd(m) return m.ackd end
function mm.ack(m) return ack_msg(m) end
function mm.ack_next(m) return ack_msg(m, "+NXT") end
function mm.ack_ack(m, rib) return ack_msg(m, "+ACK", nil, true) end
function mm.nak(m, d) return ack_msg(m, "-NAK", d) end
function mm.term(m) return ack_msg(m, "+TERM") end
function mm.in_progress(m) return ack_msg(m, "+WIP") end
mm.wip = mm.in_progress

local str_gmatch = string.gmatch
local function str_explode(s, ss, p)
    if type(ss) ~= "string" or #ss == 0 then return error("Invalid separator.") end
    local t, i = {}, 1
    for sub in str_gmatch(s, p and ss or "([^" .. ss .. "]+)") do
        t[i] = sub
        i = i + 1
    end
    return t, i-1
end

function mm.metafields(m)
	local s, tc = str_explode(m.reply, ".") -- string, token count
	if s[1] == "$JS" and s[2] == "ACK" then
		if tc == 12 then
			return {
				domain = s[3],
				account_hash = s[4],
				stream = s[5],
				consumer = s[6],
				delivered = tonumber(s[7]),
				stream_sequence = tonumber(s[8]),
				consumer_sequence = tonumber(s[9]),
				timestamp = tonumber(s[10]),
				pending = tonumber(s[11]),
				token = tonumber(s[12]),
			}
		elseif tc == 9 then
			return {
				--domain = "", -- duplicated behavior of go client
				--account_hash = "", -- same
				stream = s[3],
				consumer = s[4],
				delivered = tonumber(s[5]),
				stream_sequence = tonumber(s[6]),
				consumer_sequence = tonumber(s[7]),
				timestamp = tonumber(s[8]),
				pending = tonumber(s[9]),
			}
		end
	else return {} end
end

-- TODO mm.delete for removing in streams (js.remove)

return nats


-- local stream_conf = {
	-- name = nil,
	-- description = nil,
	-- subjects = nil,
	-- retention = nil,
	-- max_consumers = nil,
	-- max_msgs = nil,
	-- max_bytes = nil,
	-- discard = nil,
	-- discard_new_per_subject = nil,
	-- max_age = nil,
	-- max_msgs_per_subject = nil,
	-- max_msg_size = nil,
	-- storage = nil,
	-- num_replicas = nil,
	-- no_ack = nil,
	-- duplicate_window = nil,
	-- Placement = nil,
	-- mirror = nil,
	-- sources = nil,
	-- sealed = nil,
	-- deny_delete = nil,
	-- deny_purge = nil,
	-- allow_rollup_hdrs = nil,
	-- compression = nil,
	-- first_seq = nil,
	-- subject_transform = nil,
	-- republish = nil,
	-- allow_direct = nil,
	-- mirror_direct = nil,
	-- consumer_limits = nil,
	-- metadata = nil,
	-- template_owner = nil,
-- }

-- -- All API endpoints.
-- jsAllAPI = "$JS.API.>"
-- -- For constructing JetStream domain prefixes.
-- jsDomainAPI = "$JS.%s.API.>"
-- JSApiPrefix = "$JS.API"
-- -- JSApiAccountInfo is for obtaining general information about JetStream for this account.
-- -- Will return JSON response.
-- JSApiAccountInfo = "$JS.API.INFO"
-- -- JSApiTemplateCreate is the endpoint to create new stream templates.
-- -- Will return JSON response.
-- JSApiTemplateCreate  = "$JS.API.STREAM.TEMPLATE.CREATE.*"
-- JSApiTemplateCreateT = "$JS.API.STREAM.TEMPLATE.CREATE.%s"
-- -- JSApiTemplates is the endpoint to list all stream template names for this account.
-- -- Will return JSON response.
-- JSApiTemplates = "$JS.API.STREAM.TEMPLATE.NAMES"0
-- -- JSApiTemplateInfo is for obtaining general information about a named stream template.
-- -- Will return JSON response.
-- JSApiTemplateInfo  = "$JS.API.STREAM.TEMPLATE.INFO.*"
-- JSApiTemplateInfoT = "$JS.API.STREAM.TEMPLATE.INFO.%s"
-- -- JSApiTemplateDelete is the endpoint to delete stream templates.
-- -- Will return JSON response.
-- JSApiTemplateDelete  = "$JS.API.STREAM.TEMPLATE.DELETE.*"
-- JSApiTemplateDeleteT = "$JS.API.STREAM.TEMPLATE.DELETE.%s"
-- -- JSApiStreamCreate is the endpoint to create new streams.
-- -- Will return JSON response.
-- JSApiStreamCreate  = "$JS.API.STREAM.CREATE.*"
-- JSApiStreamCreateT = "$JS.API.STREAM.CREATE.%s"
-- -- JSApiStreamUpdate is the endpoint to update existing streams.
-- -- Will return JSON response.
-- JSApiStreamUpdate  = "$JS.API.STREAM.UPDATE.*"
-- JSApiStreamUpdateT = "$JS.API.STREAM.UPDATE.%s"
-- -- JSApiStreams is the endpoint to list all stream names for this account.
-- -- Will return JSON response.
-- JSApiStreams = "$JS.API.STREAM.NAMES"
-- -- JSApiStreamList is the endpoint that will return all detailed stream information
-- JSApiStreamList = "$JS.API.STREAM.LIST"
-- -- JSApiStreamInfo is for obtaining general information about a named stream.
-- -- Will return JSON response.
-- JSApiStreamInfo  = "$JS.API.STREAM.INFO.*"
-- JSApiStreamInfoT = "$JS.API.STREAM.INFO.%s"
-- -- JSApiStreamDelete is the endpoint to delete streams.
-- -- Will return JSON response.
-- JSApiStreamDelete  = "$JS.API.STREAM.DELETE.*"
-- JSApiStreamDeleteT = "$JS.API.STREAM.DELETE.%s"
-- -- JSApiStreamPurge is the endpoint to purge streams.
-- -- Will return JSON response.
-- JSApiStreamPurge  = "$JS.API.STREAM.PURGE.*"
-- JSApiStreamPurgeT = "$JS.API.STREAM.PURGE.%s"
-- -- JSApiStreamSnapshot is the endpoint to snapshot streams.
-- -- Will return a stream of chunks with a nil chunk as EOF to
-- -- the deliver subject. Caller should respond to each chunk
-- -- with a nil body response for ack flow.
-- JSApiStreamSnapshot  = "$JS.API.STREAM.SNAPSHOT.*"
-- JSApiStreamSnapshotT = "$JS.API.STREAM.SNAPSHOT.%s"
-- -- JSApiStreamRestore is the endpoint to restore a stream from a snapshot.
-- -- Caller should respond to each chunk with a nil body response.
-- JSApiStreamRestore  = "$JS.API.STREAM.RESTORE.*"
-- JSApiStreamRestoreT = "$JS.API.STREAM.RESTORE.%s"
-- -- JSApiMsgDelete is the endpoint to delete messages from a stream.
-- -- Will return JSON response.
-- JSApiMsgDelete  = "$JS.API.STREAM.MSG.DELETE.*"
-- JSApiMsgDeleteT = "$JS.API.STREAM.MSG.DELETE.%s"
-- -- JSApiMsgGet is the template for direct requests for a message by its stream sequence number.
-- -- Will return JSON response.
-- JSApiMsgGet  = "$JS.API.STREAM.MSG.GET.*"
-- JSApiMsgGetT = "$JS.API.STREAM.MSG.GET.%s"
-- -- JSDirectMsgGet is the template for non-api layer direct requests for a message by its stream sequence number or last by subject.
-- -- Will return the message similar to how a consumer receives the message, no JSON processing.
-- -- If the message can not be found we will use a status header of 404. If the stream does not exist the client will get a no-responders or timeout.
-- JSDirectMsgGet  = "$JS.API.DIRECT.GET.*"
-- JSDirectMsgGetT = "$JS.API.DIRECT.GET.%s"
-- -- This is a direct version of get last by subject, which will be the dominant pattern for KV access once 2.9 is released.
-- -- The stream and the key will be part of the subject to allow for no-marshal payloads and subject based security permissions.
-- JSDirectGetLastBySubject  = "$JS.API.DIRECT.GET.*.>"
-- JSDirectGetLastBySubjectT = "$JS.API.DIRECT.GET.%s.%s"
-- -- jsDirectGetPre
-- jsDirectGetPre = "$JS.API.DIRECT.GET"
-- -- JSApiConsumerCreate is the endpoint to create consumers for streams.
-- -- This was also the legacy endpoint for ephemeral consumers.
-- -- It now can take consumer name and optional filter subject, which when part of the subject controls access.
-- -- Will return JSON response.
-- JSApiConsumerCreate    = "$JS.API.CONSUMER.CREATE.*"
-- JSApiConsumerCreateT   = "$JS.API.CONSUMER.CREATE.%s"
-- JSApiConsumerCreateEx  = "$JS.API.CONSUMER.CREATE.*.>"
-- JSApiConsumerCreateExT = "$JS.API.CONSUMER.CREATE.%s.%s.%s"
-- -- JSApiDurableCreate is the endpoint to create durable consumers for streams.
-- -- You need to include the stream and consumer name in the subject.
-- JSApiDurableCreate  = "$JS.API.CONSUMER.DURABLE.CREATE.*.*"
-- JSApiDurableCreateT = "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s"
-- -- JSApiConsumers is the endpoint to list all consumer names for the stream.
-- -- Will return JSON response.
-- JSApiConsumers  = "$JS.API.CONSUMER.NAMES.*"
-- JSApiConsumersT = "$JS.API.CONSUMER.NAMES.%s"
-- -- JSApiConsumerList is the endpoint that will return all detailed consumer information
-- JSApiConsumerList  = "$JS.API.CONSUMER.LIST.*"
-- JSApiConsumerListT = "$JS.API.CONSUMER.LIST.%s"
-- -- JSApiConsumerInfo is for obtaining general information about a consumer.
-- -- Will return JSON response.
-- JSApiConsumerInfo  = "$JS.API.CONSUMER.INFO.*.*"
-- JSApiConsumerInfoT = "$JS.API.CONSUMER.INFO.%s.%s"
-- -- JSApiConsumerDelete is the endpoint to delete consumers.
-- -- Will return JSON response.
-- JSApiConsumerDelete  = "$JS.API.CONSUMER.DELETE.*.*"
-- JSApiConsumerDeleteT = "$JS.API.CONSUMER.DELETE.%s.%s"
-- -- JSApiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
-- JSApiRequestNextT = "$JS.API.CONSUMER.MSG.NEXT.%s.%s"
-- -- jsRequestNextPre
-- jsRequestNextPre = "$JS.API.CONSUMER.MSG.NEXT."
-- -- For snapshots and restores. The ack will have additional tokens.
-- jsSnapshotAckT    = "$JS.SNAPSHOT.ACK.%s.%s"
-- jsRestoreDeliverT = "$JS.SNAPSHOT.RESTORE.%s.%s"
-- -- JSApiStreamRemovePeer is the endpoint to remove a peer from a clustered stream and its consumers.
-- -- Will return JSON response.
-- JSApiStreamRemovePeer  = "$JS.API.STREAM.PEER.REMOVE.*"
-- JSApiStreamRemovePeerT = "$JS.API.STREAM.PEER.REMOVE.%s"
-- -- JSApiStreamLeaderStepDown is the endpoint to have stream leader stepdown.
-- -- Will return JSON response.
-- JSApiStreamLeaderStepDown  = "$JS.API.STREAM.LEADER.STEPDOWN.*"
-- JSApiStreamLeaderStepDownT = "$JS.API.STREAM.LEADER.STEPDOWN.%s"
-- -- JSApiConsumerLeaderStepDown is the endpoint to have consumer leader stepdown.
-- -- Will return JSON response.
-- JSApiConsumerLeaderStepDown  = "$JS.API.CONSUMER.LEADER.STEPDOWN.*.*"
-- JSApiConsumerLeaderStepDownT = "$JS.API.CONSUMER.LEADER.STEPDOWN.%s.%s"
-- -- JSApiLeaderStepDown is the endpoint to have our metaleader stepdown.
-- -- Only works from system account.
-- -- Will return JSON response.
-- JSApiLeaderStepDown = "$JS.API.META.LEADER.STEPDOWN"
-- -- JSApiRemoveServer is the endpoint to remove a peer server from the cluster.
-- -- Only works from system account.
-- -- Will return JSON response.
-- JSApiRemoveServer = "$JS.API.SERVER.REMOVE"
-- -- JSApiAccountPurge is the endpoint to purge the js content of an account
-- -- Only works from system account.
-- -- Will return JSON response.
-- JSApiAccountPurge  = "$JS.API.ACCOUNT.PURGE.*"
-- JSApiAccountPurgeT = "$JS.API.ACCOUNT.PURGE.%s"
-- -- JSApiServerStreamMove is the endpoint to move streams off a server
-- -- Only works from system account.
-- -- Will return JSON response.
-- JSApiServerStreamMove  = "$JS.API.ACCOUNT.STREAM.MOVE.*.*"
-- JSApiServerStreamMoveT = "$JS.API.ACCOUNT.STREAM.MOVE.%s.%s"
-- -- JSApiServerStreamCancelMove is the endpoint to cancel a stream move
-- -- Only works from system account.
-- -- Will return JSON response.
-- JSApiServerStreamCancelMove  = "$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.*"
-- JSApiServerStreamCancelMoveT = "$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.%s.%s"
-- -- The prefix for system level account API.
-- jsAPIAccountPre = "$JS.API.ACCOUNT."
-- -- jsAckT is the template for the ack message stream coming back from a consumer
-- -- when they ACK/NAK, etc a message.
-- jsAckT      = "$JS.ACK.%s.%s"
-- jsAckPre    = "$JS.ACK."
-- jsAckPreLen = len(jsAckPre)
-- -- jsFlowControl is for flow control subjects.
-- jsFlowControlPre = "$JS.FC."
-- -- jsFlowControl is for FC responses.
-- jsFlowControl = "$JS.FC.%s.%s.*"
-- -- JSAdvisoryPrefix is a prefix for all JetStream advisories.
-- JSAdvisoryPrefix = "$JS.EVENT.ADVISORY"
-- -- JSMetricPrefix is a prefix for all JetStream metrics.
-- JSMetricPrefix = "$JS.EVENT.METRIC"
-- -- JSMetricConsumerAckPre is a metric containing ack latency.
-- JSMetricConsumerAckPre = "$JS.EVENT.METRIC.CONSUMER.ACK"
-- -- JSAdvisoryConsumerMaxDeliveryExceedPre is a notification published when a message exceeds its delivery threshold.
-- JSAdvisoryConsumerMaxDeliveryExceedPre = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES"
-- -- JSAdvisoryConsumerMsgNakPre is a notification published when a message has been naked
-- JSAdvisoryConsumerMsgNakPre = "$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED"
-- -- JSAdvisoryConsumerMsgTerminatedPre is a notification published when a message has been terminated.
-- JSAdvisoryConsumerMsgTerminatedPre = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED"
-- -- JSAdvisoryStreamCreatedPre notification that a stream was created.
-- JSAdvisoryStreamCreatedPre = "$JS.EVENT.ADVISORY.STREAM.CREATED"
-- -- JSAdvisoryStreamDeletedPre notification that a stream was deleted.
-- JSAdvisoryStreamDeletedPre = "$JS.EVENT.ADVISORY.STREAM.DELETED"
-- -- JSAdvisoryStreamUpdatedPre notification that a stream was updated.
-- JSAdvisoryStreamUpdatedPre = "$JS.EVENT.ADVISORY.STREAM.UPDATED"
-- -- JSAdvisoryConsumerCreatedPre notification that a template created.
-- JSAdvisoryConsumerCreatedPre = "$JS.EVENT.ADVISORY.CONSUMER.CREATED"
-- -- JSAdvisoryConsumerDeletedPre notification that a template deleted.
-- JSAdvisoryConsumerDeletedPre = "$JS.EVENT.ADVISORY.CONSUMER.DELETED"
-- -- JSAdvisoryStreamSnapshotCreatePre notification that a snapshot was created.
-- JSAdvisoryStreamSnapshotCreatePre = "$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_CREATE"
-- -- JSAdvisoryStreamSnapshotCompletePre notification that a snapshot was completed.
-- JSAdvisoryStreamSnapshotCompletePre = "$JS.EVENT.ADVISORY.STREAM.SNAPSHOT_COMPLETE"
-- -- JSAdvisoryStreamRestoreCreatePre notification that a restore was start.
-- JSAdvisoryStreamRestoreCreatePre = "$JS.EVENT.ADVISORY.STREAM.RESTORE_CREATE"
-- -- JSAdvisoryStreamRestoreCompletePre notification that a restore was completed.
-- JSAdvisoryStreamRestoreCompletePre = "$JS.EVENT.ADVISORY.STREAM.RESTORE_COMPLETE"
-- -- JSAdvisoryStreamLeaderElectedPre notification that a replicated stream has elected a leader.
-- JSAdvisoryStreamLeaderElectedPre = "$JS.EVENT.ADVISORY.STREAM.LEADER_ELECTED"
-- -- JSAdvisoryStreamQuorumLostPre notification that a stream and its consumers are stalled.
-- JSAdvisoryStreamQuorumLostPre = "$JS.EVENT.ADVISORY.STREAM.QUORUM_LOST"
-- -- JSAdvisoryConsumerLeaderElectedPre notification that a replicated consumer has elected a leader.
-- JSAdvisoryConsumerLeaderElectedPre = "$JS.EVENT.ADVISORY.CONSUMER.LEADER_ELECTED"
-- -- JSAdvisoryConsumerQuorumLostPre notification that a consumer is stalled.
-- JSAdvisoryConsumerQuorumLostPre = "$JS.EVENT.ADVISORY.CONSUMER.QUORUM_LOST"
-- -- JSAdvisoryServerOutOfStorage notification that a server has no more storage.
-- JSAdvisoryServerOutOfStorage = "$JS.EVENT.ADVISORY.SERVER.OUT_OF_STORAGE"
-- -- JSAdvisoryServerRemoved notification that a server has been removed from the system.
-- JSAdvisoryServerRemoved = "$JS.EVENT.ADVISORY.SERVER.REMOVED"
-- -- JSAuditAdvisory is a notification about JetStream API access.
-- -- FIXME - Add in details about who..
-- JSAuditAdvisory = "$JS.EVENT.ADVISORY.API"