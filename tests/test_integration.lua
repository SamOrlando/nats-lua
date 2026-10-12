-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Integration tests against a live NATS server (JetStream enabled, no auth).
-- usage: NATS_URL=nats://host:4222 lua tests/test_integration.lua
-- Everything lives under the subject prefix nats-lua.test.<random>.> and the
-- streams/buckets it creates are deleted at the end.
package.path = "./?.lua;./tests/?.lua;" .. package.path

if os.getenv("NATS_NO_CJSON") then -- exercise the pure Lua JSON fallback
    package.preload.cjson = function() error("cjson disabled for test") end
end
local nats = require("nats-kvs") -- loads nats + nats-jetstream too
require("nats-objs")
local nuid = require("nuid")
local socket = require("socket")

local URL = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"
local RID = nuid.next():sub(1, 10)
local P = "nats-lua.test." .. RID
local STREAM = "nats-lua-test-" .. RID
local BUCKET = "nats-lua-test-" .. RID

local passed, failed, skipped = 0, 0, 0
local cleanup = {}
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then passed = passed + 1; print("ok   " .. name)
    elseif tostring(err):find("^SKIP") then skipped = skipped + 1; print("skip " .. name .. ": " .. tostring(err))
    else failed = failed + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, msg)
    if a ~= b then error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end
end

local function connect(extra)
    local o = { servers = { URL }, name = "nats-lua-test", timeout = 3 }
    for k, v in pairs(extra or {}) do o[k] = v end
    local c = nats.client(o)
    assert(c:connect())
    return c
end

local nc
test("connect + INFO", function()
    nc = connect()
    local si = nc.server_information
    assert(si.server_id and si.version, "INFO received")
    assert(si.headers, "server supports headers")
    print("     server " .. tostring(si.server_name) .. " v" .. tostring(si.version) ..
        " cluster=" .. tostring(si.cluster) .. " jetstream=" .. tostring(si.jetstream) ..
        " max_payload=" .. tostring(si.max_payload))
    assert(nc:flush(2))
end)
if not nc then print("cannot connect to " .. URL); os.exit(1) end

test("pub/sub with echo, binary payload", function()
    local got
    local sid = nc:subscribe(P .. ".ps", function(m) got = m end)
    nc:flush()
    local payload = "a\r\nb\0\255" .. string.rep("z", 1000)
    nc:publish(P .. ".ps", payload)
    nc:wait({ sid = sid, timeout = 2 })
    assert(got, "message received")
    eq(got.data, payload)
    nc:unsubscribe(sid)
end)

test("wildcards and queue groups", function()
    local c2 = connect()
    local w, q1, q2 = 0, 0, 0
    nc:subscribe(P .. ".wc.*.x", function() w = w + 1 end)
    local s1 = nc:subscribe(P .. ".q", function() q1 = q1 + 1 end, "workers")
    local s2 = c2:subscribe(P .. ".q", function() q2 = q2 + 1 end, "workers")
    nc:flush(); c2:flush()
    nc:publish(P .. ".wc.a.x", "1")
    nc:publish(P .. ".wc.b.x", "2")
    nc:publish(P .. ".wc.b.y", "no")
    for i = 1, 20 do nc:publish(P .. ".q", tostring(i)) end
    nc:flush()
    nc:wait({ timeout = 0.5 })
    c2:wait({ timeout = 0.5 })
    eq(w, 2, "wildcard deliveries")
    eq(q1 + q2, 20, "queue group delivers each message once")
    nc:unsubscribe(s1); c2:close()
end)

test("headers round trip (HPUB/HMSG)", function()
    local got
    local sid = nc:subscribe(P .. ".hdr", function(m) got = m end)
    nc:flush()
    nc:publish(P .. ".hdr", "body", nil, { ["X-One"] = "1", ["X-Multi"] = { "a", "b" } })
    nc:wait({ sid = sid, timeout = 2 })
    local h = got:headers()
    eq(got.data, "body"); eq(h["X-One"], "1"); eq(h["X-Multi"][1], "a"); eq(h["X-Multi"][2], "b")
    nc:unsubscribe(sid)
end)

test("request/reply across connections", function()
    local svc = connect()
    svc:subscribe(P .. ".svc.echo", function(m) m:respond("re:" .. m.data) end)
    svc:flush()
    -- the requester pumps its own socket, the service needs pumping too:
    local co = coroutine.wrap(function() end)
    local r
    local m = nc:request(P .. ".svc.echo", function(x) r = x.data end, "hello")
    local deadline = socket.gettime() + 3
    while not r and socket.gettime() < deadline do
        svc:wait({ timeout = 0.05 })
        nc:wait({ lid = m.lid, timeout = 0.05 })
    end
    eq(r, "re:hello")
    svc:close()
end)

test("get() with responder on same connection", function()
    local sid = nc:subscribe(P .. ".svc.same", function(m) m:respond("pong") end)
    nc:flush()
    local r, err = nc:get(P .. ".svc.same", "ping")
    assert(r, err); eq(r.data, "pong")
    nc:unsubscribe(sid)
end)

test("no responders (503)", function()
    local r, err = nc:get(P .. ".nobody.home", "x")
    eq(r, nil)
    if err == "timeout" then error("SKIP server sent no 503 status (raw protocol check shows the same)", 0) end
    eq(err, "no responders")
end)

test("max_payload enforced locally", function()
    local maxp = nc.server_information.max_payload
    local ok, err = pcall(nc.publish, nc, P .. ".big", string.rep("x", maxp + 1))
    assert(not ok and tostring(err):find("maximum payload"), tostring(err))
    nc:publish(P .. ".big", string.rep("x", math.min(maxp, 256 * 1024)))
    assert(nc:flush(3), "still connected after large publish")
end)

test("auto-unsubscribe after max", function()
    local n = 0
    local sid = nc:subscribe(P .. ".max", function() n = n + 1 end)
    nc:unsubscribe(sid, 3)
    for _ = 1, 10 do nc:publish(P .. ".max", "x") end
    nc:flush()
    nc:wait({ timeout = 0.5 })
    eq(n, 3); eq(nc.subscriptions[sid], nil)
end)

test("on-demand reconnect resubscribes", function()
    local c = connect({ reconnect = true, reconnect_wait = 0.1 })
    local ups, got = 0, 0
    c.config.reconnected_cb = function() ups = ups + 1 end
    local sid = c:subscribe(P .. ".rc", function() got = got + 1 end)
    c:flush()
    c:reconnect()
    eq(ups, 1); eq(c.stats.reconnects, 1)
    assert(c:flush(2), "flush after reconnect")
    nc:publish(P .. ".rc", "after")
    nc:flush()
    c:wait({ sid = sid, timeout = 2 })
    eq(got, 1, "message received on resubscribed sid")
    c:close()
end)

test("drain delivers in-flight messages then closes", function()
    local c = connect()
    local n = 0
    c:subscribe(P .. ".drain", function() n = n + 1 end)
    c:flush()
    for _ = 1, 50 do nc:publish(P .. ".drain", "x") end
    nc:flush()
    socket.sleep(0.2)
    assert(c:drain(5))
    eq(n, 50); eq(c.closed, true)
end)

test("keepalive PING/PONG under wait()", function()
    local c = connect({ ping_interval = 0.1 })
    c:wait({ timeout = 0.5 })
    assert(c.pings_sent >= 3, "pings sent " .. c.pings_sent)
    assert(c.pongs_received >= c.pings_sent - 1, "pongs received")
    eq(c.connected, true)
    c:close()
end)

-- JetStream ---------------------------------------------------------------
-- probe: a clustered server without meta leader answers 503 "temporarily unavailable"
local js_ok, js_why = nc.server_information.jetstream, "server without JetStream"
if js_ok then
    local info, err = nc:account_info()
    if not info then
        js_ok = false
        js_why = type(err) == "table" and err.data.error.description or tostring(err)
    end
end
print("     JetStream usable: " .. tostring(js_ok and true or false) .. (js_ok and "" or (" (" .. js_why .. ")")))
local stream
test("JetStream: create stream", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local err
    stream, err = nc:stream({ name = STREAM, subjects = { P .. ".js.>" }, storage = "memory",
        max_age = 3600 * nats.jetstream.NS }, "create")
    assert(stream, type(err) == "table" and nats.json.encode(err.data) or tostring(err))
    cleanup[#cleanup + 1] = function() nc:delete_stream(STREAM) end
    eq(stream.name, STREAM)
    local info = nc:get_stream(STREAM)
    assert(info and info.config.subjects[1] == P .. ".js.>")
end)

test("JetStream: publish with pub ack, dedupe, expected stream", function()
    if not stream then error("SKIP no stream", 0) end
    local ack, err = stream:publish(P .. ".js.a", "one", { id = "m1" })
    assert(ack, err); eq(ack.stream, STREAM); eq(ack.seq, 1)
    local dup = stream:publish(P .. ".js.a", "one", { id = "m1" })
    eq(dup.duplicate, true)
    ack = nc:js_publish(P .. ".js.b", "two"); eq(ack.seq, 2)
    local bad, berr = nc:js_publish(P .. ".js.b", "x", { stream = "other-stream" })
    eq(bad, nil); assert(berr, "expected stream mismatch rejected")
    for i = 3, 5 do assert(stream:publish(P .. ".js.c", "n" .. i)) end
end)

test("JetStream: stream info, getmsg, purge by subject", function()
    if not stream then error("SKIP no stream", 0) end
    local s = stream:info()
    eq(s.state.messages, 5)
    local m = stream:getmsg(2)
    assert(m and m.data, "got message 2")
    local ok = stream:purge(P .. ".js.c")
    assert(ok)
    s = stream:info()
    eq(s.state.messages, 2)
end)

test("JetStream: pull consumer fetch + ack", function()
    if not stream then error("SKIP no stream", 0) end
    local con, err = stream:consumer({ durable_name = "c1", ack_policy = "explicit" }, "create")
    assert(con, type(err) == "table" and nats.json.encode(err.data) or tostring(err))
    local msgs = con:fetch(5, nil, 1)
    eq(#msgs, 2)
    eq(msgs[1].data, "one"); eq(msgs[2].data, "two")
    local meta = msgs[1]:metafields()
    eq(meta.stream, STREAM); eq(meta.consumer, "c1"); eq(meta.stream_seq, 1)
    for _, m in ipairs(msgs) do assert(m:ack()) end
    assert(msgs[2]:acked())
    local none, nerr = con:fetch(1, nil, 0) -- no_wait
    eq(#none, 0)
    local info = con:info()
    eq(info.num_ack_pending, 0)
    assert(con:delete())
end)

test("JetStream: double ack and nak redelivery", function()
    if not stream then error("SKIP no stream", 0) end
    local con = assert(stream:consumer({ name = "c2", ack_policy = "explicit", ack_wait = 30 * nats.jetstream.NS }, "create"))
    local m = con:next(1)
    assert(m, "message")
    assert(m:nak())
    nc:flush()
    socket.sleep(0.1)
    local again = con:next(1)
    assert(again and again.data == m.data, "redelivered after nak")
    eq(again:metafields().delivered, 2)
    local r = again:ack_ack()
    assert(r, "ack ack confirmed")
    assert(con:delete())
end)

-- KV ------------------------------------------------------------------------
test("KV: create bucket, put/get/update/delete/purge, destroy", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local kv, err = nc:kvs({ bucket = BUCKET, history = 5, storage = "memory" })
    assert(kv, type(err) == "table" and nats.json.encode(err.data) or tostring(err))
    cleanup[#cleanup + 1] = function() nc:delete_stream("KV_" .. BUCKET) end
    local p = assert(kv:put("a.b", "v1"))
    eq(p.revision, 1)
    local e = assert(kv:get("a.b"))
    eq(e.value, "v1"); eq(e.revision, 1); eq(e.op, "PUT")
    assert(kv:update("a.b", "v2", 1))
    local bad, berr = kv:update("a.b", "v3", 1)
    eq(bad, nil); assert(berr, "wrong revision rejected")
    eq(kv:get("a.b").value, "v2")
    eq(kv:get("a.b", 1).value, "v1", "get by revision")
    local c1 = kv:create("new", "x"); assert(c1)
    local c2 = kv:create("new", "y"); eq(c2, nil, "create on existing key fails")
    local miss, merr = kv:get("missing")
    eq(miss, nil); eq(merr, "key not found")
    assert(kv:delete("a.b"))
    local _, derr, de = kv:get("a.b")
    eq(derr, "key not found"); eq(de.op, "DEL")
    assert(kv:purge("new"))
    local _, _, pe = kv:get("new")
    eq(pe.op, "PURGE")
    local st = kv:status()
    eq(st.history, 5)
    local seen
    local wsid = kv:watch_all(function(m) seen = m.subject end, { raw = true }) -- legacy raw watch
    nc:flush()
    kv:put("w.k", "1")
    nc:wait({ sid = wsid, timeout = 2 })
    eq(seen, "$KV." .. BUCKET .. ".w.k")
    nc:unsubscribe(wsid)
    assert(kv:destroy())
    table.remove(cleanup)
end)

-- Object store ---------------------------------------------------------------
local OBUCKET, OBUCKET2 = BUCKET .. "-obj", BUCKET .. "-obj2"
local obs, obs2
local function blob(n)
    local t = {}
    for i = 1, n do t[i] = string.char((i * 7 + math.floor(i / 251)) % 256) end
    return table.concat(t)
end

test("OBJ: create bucket (ADR-20 stream config)", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local err
    obs, err = nc:create_object_store({ bucket = OBUCKET, description = "nats-lua test", storage = "memory",
        metadata = { owner = "nats-lua" } })
    assert(obs, type(err) == "table" and nats.json.encode(err.data) or tostring(err))
    cleanup[#cleanup + 1] = function() nc:delete_object_store(OBUCKET) end
    local c = obs.stream.config
    eq(c.name, "OBJ_" .. OBUCKET); eq(c.allow_rollup_hdrs, true); eq(c.allow_direct, true); eq(c.discard, "new")
    obs2 = assert(nc:create_object_store({ bucket = OBUCKET2, storage = "memory" }))
    cleanup[#cleanup + 1] = function() nc:delete_object_store(OBUCKET2) end
    assert(nc:object_store(OBUCKET), "bind to existing bucket")
end)

test("OBJ: put/get small, empty, multi-chunk, streaming", function()
    if not obs then error("SKIP no object store", 0) end
    local info = assert(obs:put("hello.txt", "hello world"))
    eq(info.size, 11); eq(info.chunks, 1)
    eq(info.digest, nats.objs.digest_value(nats.objs.sha256.digest("hello world")))
    local data, gi = obs:get("hello.txt")
    eq(data, "hello world"); eq(gi.nuid, info.nuid); assert(gi.mtime)
    local e = assert(obs:put("empty", ""))
    eq(e.chunks, 0); eq(e.size, 0)
    eq(obs:get("empty"), "")
    local big = blob(300 * 1024 + 17)
    local bi = assert(obs:put({ name = "dir/big.bin", description = "big", headers = { ["X-A"] = { "1" } } },
        big, { chunk_size = 64 * 1024 }))
    eq(bi.chunks, 5); eq(bi.size, #big)
    eq(obs:get("dir/big.bin"), big)
    local parts = {}
    assert(obs:get("dir/big.bin", { callback = function(c) parts[#parts + 1] = c end, batch = 2 }))
    eq(#parts, 5); eq(table.concat(parts), big)
    local i2 = assert(obs:get_info("dir/big.bin"))
    eq(i2.description, "big"); eq(i2.headers["X-A"][1], "1"); eq(i2.options.max_chunk_size, 64 * 1024)
    -- reader function input
    local n = 0
    local ri = assert(obs:put("reader", function() n = n + 1 if n <= 3 then return string.rep("r", 1000) end end,
        { chunk_size = 1024 }))
    eq(ri.size, 3000); eq(ri.chunks, 3)
end)

test("OBJ: overwrite purges old chunks", function()
    if not obs then error("SKIP no object store", 0) end
    local before = obs:status().stream.state.messages
    local a = assert(obs:put("ow", string.rep("a", 5000), { chunk_size = 1000 }))
    local mid = obs:status().stream.state.messages
    eq(mid, before + 6)
    local b = assert(obs:put("ow", "b"))
    assert(a.nuid ~= b.nuid)
    eq(obs:status().stream.state.messages, before + 2, "5 old chunks purged, meta rolled up")
    eq(obs:get("ow"), "b")
end)

test("OBJ: put_file / get_file", function()
    if not obs then error("SKIP no object store", 0) end
    local src, dst = os.tmpname(), os.tmpname()
    local f = io.open(src, "wb"); f:write(blob(70000)); f:close()
    local info = assert(obs:put_file(src, "file.bin", { chunk_size = 32768 }))
    eq(info.chunks, 3)
    assert(obs:get_file("file.bin", dst))
    local a, b = io.open(src, "rb"), io.open(dst, "rb")
    eq(b:read("*a"), a:read("*a")); a:close(); b:close()
    os.remove(src); os.remove(dst)
end)

test("OBJ: update_meta rename, delete, show_deleted, list", function()
    if not obs then error("SKIP no object store", 0) end
    assert(obs:put("tmp", "data"))
    assert(obs:update_meta("tmp", { name = "renamed", description = "new desc" }))
    local _, e1 = obs:get_info("tmp"); eq(e1, "object not found")
    eq(obs:get_info("renamed").description, "new desc")
    eq(obs:get("renamed"), "data")
    local r, e2 = obs:update_meta("renamed", { name = "hello.txt" }); eq(r, nil); assert(e2)
    assert(obs:delete("renamed"))
    local _, e3 = obs:get("renamed"); eq(e3, "object not found")
    local d = assert(obs:get_info("renamed", true)); eq(d.deleted, true); eq(d.size, 0)
    local _, e4 = obs:update_meta("renamed", { description = "x" }); assert(e4)
    local list = assert(obs:list())
    local names = {}
    for _, i in ipairs(list) do names[i.name] = true end
    assert(names["hello.txt"] and names["dir/big.bin"] and names["empty"] and not names["renamed"], "list")
    local all = assert(obs:list({ show_deleted = true }))
    eq(#all, #list + 1)
    local _, e5 = obs:delete("never-existed"); assert(e5)
end)

test("OBJ: links to objects and buckets", function()
    if not obs then error("SKIP no object store", 0) end
    local target = assert(obs:get_info("hello.txt"))
    assert(obs2:add_link("link-to-hello", target))
    eq(obs2:get("link-to-hello"), "hello world")
    assert(obs2:add_bucket_link("bucket-link", obs))
    local bl = assert(obs2:get_info("bucket-link")); eq(bl.options.link.bucket, OBUCKET)
    local _, e1 = obs2:get("bucket-link"); assert(e1, "cannot get bucket link")
    local li = assert(obs2:get_info("link-to-hello"))
    local _, e2 = obs:add_link("x", li); assert(e2, "link to link refused")
    local _, e3 = obs:add_link("hello.txt", target); assert(e3, "cannot overwrite regular object")
end)

test("OBJ: watch (initial values, nil marker, updates)", function()
    if not obs then error("SKIP no object store", 0) end
    local seen, marker = {}, false
    assert(obs:watch(function(info)
        if info == nil then marker = true return false end
        seen[#seen + 1] = info.name
    end, { timeout = 5 }))
    eq(marker, true)
    assert(#seen >= 5, "initial values " .. #seen)
    local up = {}
    obs2:put("watched", "1")
    assert(obs2:watch(function(info)
        if info then up[#up + 1] = info.name end
    end, { timeout = 1.5 }))
    local found = false
    for _, n in ipairs(up) do if n == "watched" then found = true end end
    assert(found, "watch saw existing object")
end)

test("OBJ: status, seal", function()
    if not obs then error("SKIP no object store", 0) end
    local st = assert(obs:status())
    eq(st.bucket, OBUCKET); eq(st.sealed, false); eq(st.backing_store, "JetStream"); eq(st.description, "nats-lua test")
    eq(st.metadata.owner, "nats-lua")
    local s2 = assert(obs2:seal())
    eq(s2.sealed, true)
    local r = obs2:put("after-seal", "x")
    eq(r, nil, "sealed bucket refuses writes")
end)


-- 0.3.0: KV full spec (ADR-8/48) -------------------------------------------
local function pump(secs, done)
    local t = socket.gettime() + secs
    while socket.gettime() < t do
        nc:wait({ timeout = 0.05 })
        if done and done() then return true end
    end
    return done == nil
end

test("KV: watch (initial values + marker + updates), keys, history, create-after-delete", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local B = BUCKET .. "-kv2"
    local kv = assert(nc:create_key_value({ bucket = B, history = 10, storage = "memory" }))
    cleanup[#cleanup + 1] = function() nc:delete_key_value(B) end
    kv:put("a.1", "x"); kv:put("a.2", "y"); kv:put("a.1", "x2"); kv:put("b", "z"); kv:delete("b")
    local seen, marker = {}, 0
    local w = assert(kv:watch("a.*", function(e)
        if e then seen[#seen + 1] = e.key .. "=" .. tostring(e.value) else marker = marker + 1 end
    end))
    assert(w:wait_initial(5), "end of initial data")
    eq(#seen, 2); eq(marker, 1)
    kv:put("a.3", "new")
    assert(pump(3, function() return #seen == 3 end), "update delivered")
    eq(seen[3], "a.3=new")
    w:stop()
    local all = {}
    local w2 = kv:watch_all(function(e) if e then all[#all + 1] = e.op end end, { include_history = true })
    assert(w2:wait_initial(5)); w2:stop()
    eq(#all, 6, "history watch")
    local upd = {}
    local w3 = kv:watch_all(function(e) upd[#upd + 1] = e or "marker" end, { updates_only = true })
    eq(upd[1], "marker", "updates_only marker sent at start")
    kv:put("c", "1")
    assert(pump(3, function() return #upd == 2 end)); w3:stop()
    eq(upd[2].key, "c")
    local ks = assert(kv:keys()); table.sort(ks)
    eq(table.concat(ks, ","), "a.1,a.2,a.3,c")
    eq(#assert(kv:keys("a.*")), 3)
    eq(#assert(kv:keys({ "a.1", "c" })), 2, "multiple filters (ADR-34)")
    local h = assert(kv:history("a.1"))
    eq(#h, 2); eq(h[1].value, "x"); eq(h[2].value, "x2")
    local _, kerr = kv:create("a.1", "nope"); eq(kerr, "key exists")
    assert(kv:create("b", "back"), "create over a deleted key")
    eq(kv:get("b").value, "back")
    kv:delete("c"); kv:purge("a.2")
    eq(assert(kv:purge_deletes({ older_than = 0 })), 2)
    local w4 = kv:watch_all(function() end); assert(w4:wait_initial(5)); w4:stop()
    assert(kv:destroy()); table.remove(cleanup)
end)

test("KV: limit markers + per key TTL (ADR-48), mirror and sources (ADR-57)", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local B = BUCKET .. "-ttl"
    local kv, err = nc:create_key_value({ bucket = B, storage = "memory", limit_marker_ttl = 1 })
    assert(kv, tostring(type(err) == "table" and err.data and err.data.error and err.data.error.description or err))
    cleanup[#cleanup + 1] = function() nc:delete_key_value(B) end
    eq(kv:status().limit_marker_ttl, 1)
    assert(kv:create("eph", "v", { ttl = 1 }))
    local ops = {}
    local w = kv:watch("eph", function(e) if e then ops[#ops + 1] = e.op end end)
    assert(pump(6, function() return ops[#ops] == "PURGE" end), "MaxAge marker seen as PURGE: " .. table.concat(ops, ","))
    w:stop()
    assert(kv:put("p", "1")); assert(kv:purge("p", { ttl = 1 }))
    local M, S = BUCKET .. "-mir", BUCKET .. "-src"
    local mir = assert(nc:create_key_value({ bucket = M, storage = "memory", mirror = { name = B } }))
    cleanup[#cleanup + 1] = function() nc:delete_key_value(M) end
    local src = assert(nc:create_key_value({ bucket = S, storage = "memory", sources = { { name = B } } }))
    cleanup[#cleanup + 1] = function() nc:delete_key_value(S) end
    kv:put("shared", "42")
    assert(pump(5, function() local e = src:get("shared") return e and e.value == "42" end), "sourced bucket got key")
    assert(pump(5, function() local e = mir:get("shared") return e and e.value == "42" end), "mirror read")
    eq(src.stream.config.sources[1].subject_transforms[1].dest, "$KV." .. S .. ".>")
    assert(mir:put("viamirror", "1"), "mirror writes go to origin")
    eq(kv:get("viamirror").value, "1")
end)

-- 0.3.0: JetStream consumers (ADR-9/17/37/42) --------------------------------
local JS2 = STREAM .. "-c"
local s2
test("JS: consume (continuous pull) with acks and re-pull thresholds", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    s2 = assert(nc:stream({ name = JS2, subjects = { P .. ".c.>" }, storage = "memory", allow_direct = true }, "create"))
    cleanup[#cleanup + 1] = function() nc:delete_stream(JS2) end
    for i = 1, 60 do assert(nc:js_publish(P .. ".c." .. (i % 4), tostring(i))) end
    local c = assert(s2:consumer({ name = "work", ack_policy = "explicit" }, "create"))
    local n = 0
    local ctx = c:consume(function(m) n = n + 1; m:ack() end, { max_messages = 16, expires = 2 })
    assert(pump(10, function() return n == 60 end), "consumed " .. n)
    assert(ctx.pulls >= 4, "re-pulled at threshold (" .. ctx.pulls .. ")")
    ctx:stop()
    nc:flush(); eq(c:info().num_ack_pending, 0)
    -- stale pull requests of the stopped context may still be pending on the server,
    -- so the iterator uses its own consumer
    local c2 = assert(s2:consumer({ name = "iter", ack_policy = "explicit", deliver_policy = "new" }, "create"))
    local it = assert(c2:messages({ max_messages = 5 }))
    nc:js_publish(P .. ".c.x", "late")
    local m = it:next(5)
    assert(m and m.data == "late", "messages() iterator"); m:ack(); it:stop()
    nc:flush(); eq(c2:info().num_ack_pending, 0)
end)

test("JS: ordered consumer (pull) recovers from a deleted consumer", function()
    if not s2 then error("SKIP no stream", 0) end
    local oc = s2:ordered_consumer({ filter_subject = P .. ".c.1" })
    local got, resets = {}, 0
    oc.cfg.reset_cb = function() resets = resets + 1 end
    local ctx = assert(oc:consume(function(m) got[#got + 1] = m:metafields().stream_seq end, { idle_heartbeat = 1, expires = 2 }))
    assert(pump(5, function() return #got >= 5 end))
    -- delete the server side consumer: the ordered consumer must recreate it and continue
    s2:delete_consumer(oc.consumer.name)
    for i = 1, 4 do nc:js_publish(P .. ".c.1", "after" .. i) end
    assert(pump(10, function() return #got >= 19 end), "continued after reset: " .. #got)
    ctx.stop()
    for i = 2, #got do assert(got[i] > got[i - 1], "in order, no duplicates") end
    assert(resets >= 1, "reset happened")
    local f = s2:ordered_consumer({ filter_subject = P .. ".c.2" })
    eq(#f:fetch(100, 2), 15); f:stop()
end)

test("JS: push consumer flow control + idle heartbeats (ADR-9), ordered push", function()
    if not s2 then error("SKIP no stream", 0) end
    local n, errs = 0, {}
    local sub = assert(s2:subscribe({ deliver_policy = "all", ack_policy = "none" }, function() n = n + 1 end,
        { flow_control = true, idle_heartbeat = 1, error_cb = function(_, e) errs[#errs + 1] = e end }))
    assert(pump(5, function() return n == 65 end), "push delivered " .. n)
    local before = sub.last_activity
    pump(2.5)
    assert(sub.last_activity > before, "idle heartbeats received")
    eq(#errs, 0, "no missed heartbeats")
    sub:stop()
    local on = 0
    local osub = assert(s2:subscribe({ filter_subject = P .. ".c.3" }, function() on = on + 1 end,
        { ordered = true, idle_heartbeat = 1 }))
    assert(pump(5, function() return on == 15 end), "ordered push " .. on)
    osub:stop()
end)

test("JS: consumer pause/resume and pinned priority group (ADR-42)", function()
    if not s2 then error("SKIP no stream", 0) end
    local c = assert(s2:consumer({ name = "pausable", ack_policy = "explicit" }, "create"))
    local r = assert(c:pause(os.time() + 120)); eq(r.paused, true)
    eq(c:info().paused, true)
    eq(assert(c:resume()).paused, false)
    local pc, perr = s2:consumer({ name = "pinned", ack_policy = "explicit", priority_groups = { "g1" },
        priority_policy = "pinned_client", priority_timeout = 30 * nats.jetstream.NS }, "create")
    assert(pc, tostring(type(perr) == "table" and perr.data and perr.data.error and perr.data.error.description or perr))
    local msgs = pc:fetch(2, nil, 2, nil, { group = "g1" })
    eq(#msgs, 2)
    assert(pc.pin_id, "pin id received")
    for _, m in ipairs(msgs) do m:ack() end
    assert(pc:unpin("g1"))
end)

test("JS: direct get batch / multi_last (ADR-31), per message TTL (ADR-43)", function()
    if not s2 then error("SKIP no stream", 0) end
    local msgs, eob = assert(s2:direct_get({ batch = 10, seq = 1, next_by_subj = P .. ".c.>" }))
    eq(#msgs, 10); assert(eob.num_pending and eob.num_pending > 0, "EOB pending")
    local last = assert(s2:direct_get({ multi_last = { P .. ".c.*" } }))
    eq(#last, 5)
    local T = STREAM .. "-ttl"
    local ts = assert(nc:stream({ name = T, subjects = { P .. ".t.>" }, storage = "memory", allow_msg_ttl = true,
        allow_direct = true }, "create"))
    cleanup[#cleanup + 1] = function() nc:delete_stream(T) end
    assert(nc:js_publish(P .. ".t.x", "gone soon", { ttl = 1 }))
    assert(nc:js_publish(P .. ".t.y", "stays", { ttl = "never" }))
    assert(pump(5, function() local m = ts:getmsg(P .. ".t.x") return m == nil end), "TTL expired")
    assert(ts:getmsg(P .. ".t.y"), "never expires")
end)

test("JS: counters (ADR-49) and atomic batch publish (ADR-50)", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local C = STREAM .. "-cnt"
    local cs, cerr = nc:stream({ name = C, subjects = { P .. ".n.>" }, storage = "memory", allow_msg_counter = true,
        allow_direct = true }, "create")
    if not cs then error("SKIP counters unsupported: " .. tostring(type(cerr) == "table" and cerr.data and cerr.data.error and cerr.data.error.description or cerr), 0) end
    cleanup[#cleanup + 1] = function() nc:delete_stream(C) end
    eq(assert(nc:js_counter_add(P .. ".n.hits", 5)), 5)
    eq(assert(nc:js_counter_add(P .. ".n.hits", -2)), 3)
    eq(assert(cs:counter_get(P .. ".n.hits")), 3)
    local A = STREAM .. "-atom"
    local as, aerr = nc:stream({ name = A, subjects = { P .. ".b.>" }, storage = "memory", allow_atomic = true }, "create")
    if not as then error("SKIP atomic batch unsupported: " .. tostring(type(aerr) == "table" and aerr.data and aerr.data.error and aerr.data.error.description or aerr), 0) end
    cleanup[#cleanup + 1] = function() nc:delete_stream(A) end
    local batch = {}
    for i = 1, 5 do batch[i] = { subject = P .. ".b." .. i, data = "v" .. i } end
    local ack = assert(nc:js_publish_batch(batch))
    eq(ack.count, 5); eq(as:info().state.messages, 5)
end)

-- 0.3.0: core (ADR-40/47), services (ADR-32), object store watcher --------------
test("core: request_many (ADR-47) and reconnect callbacks + buffering on forced reconnect", function()
    -- request_many against a responder on the same connection
    local r1 = nc:subscribe(P .. ".rm", function(m) m:respond("1") end)
    local r2 = nc:subscribe(P .. ".rm", function(m) m:respond("2") end)
    nc:flush()
    local res = nc:request_many(P .. ".rm", "", { max_wait = 2, stall = 0.3 })
    eq(#res, 2)
    nc:unsubscribe(r1); nc:unsubscribe(r2)
    local ev = {}
    local c3 = connect({ reconnect = true, reconnect_wait = 0.05,
        disconnected_cb = function() ev[#ev + 1] = "down" end, reconnected_cb = function() ev[#ev + 1] = "up" end })
    local got = 0
    c3:subscribe(P .. ".rc", function() got = got + 1 end)
    c3:flush()
    c3:reconnect()
    for i = 1, 3 do c3:publish(P .. ".rc", tostring(i)) end
    local t = socket.gettime() + 3
    while got < 3 and socket.gettime() < t do c3:wait({ timeout = 0.1 }) end
    eq(got, 3); eq(ev[1], "down"); eq(ev[2], "up")
    c3:close()
end)

test("services (ADR-32): endpoints, discovery, stats, errors", function()
    require("nats-micro")
    local svc = assert(nc:add_service({ name = BUCKET .. "-svc", version = "1.0.0-rc.1+b5", description = "test",
        metadata = { k = "v" }, stats_handler = function() return { custom = true } end }))
    cleanup[#cleanup + 1] = function() svc:stop() end
    local g = svc:add_group(P .. ".svc")
    assert(g:add_endpoint("echo", function(req) req:respond("re:" .. req.data) end))
    assert(g:add_endpoint("bad", function(req) req:respond_error(422, "nope") end))
    assert(g:add_endpoint("boom", function() error("kaput") end))
    assert(svc:add_endpoint("noq", function(req) req:respond("x") end, { subject = P .. ".svc.noq", queue_group = false }))
    nc:flush()
    eq(nc:get(P .. ".svc.echo", "hi").data, "re:hi")
    local e = nc:get(P .. ".svc.bad", "")
    local code, desc = nats.micro.response_error(e)
    eq(code, 422); eq(desc, "nope")
    eq(nats.micro.response_error(nc:get(P .. ".svc.boom", "")), 500)
    local pings = nats.micro.ping(nc, svc.name)
    eq(#pings, 1); eq(pings[1].id, svc.id); eq(pings[1].type, "io.nats.micro.v1.ping_response")
    local info = nats.micro.info(nc, svc.name, svc.id)[1]
    eq(#info.endpoints, 4); eq(info.metadata.k, "v")
    local noq
    for _, ep in ipairs(info.endpoints) do if ep.name == "noq" then noq = ep end end
    eq(noq.queue_group, "")
    local st = nats.micro.stats(nc, svc.name)[1]
    local tot, errs = 0, 0
    for _, ep in ipairs(st.endpoints) do
        tot = tot + ep.num_requests; errs = errs + ep.num_errors
        if ep.name == "echo" then eq(ep.data.custom, true) end
    end
    eq(tot, 3); eq(errs, 2)
    assert(st.started:find("^%d+%-%d+%-%d+T"), "started timestamp")
    svc:reset(); eq(svc:stats().endpoints[1].num_requests, 0)
    svc:stop()
    eq(#(nats.micro.ping(nc, svc.name) or {}), 0, "stopped service no longer answers")
    table.remove(cleanup)
end)

test("OBJ: non blocking watcher on ordered consumers", function()
    if not obs then error("SKIP no object store", 0) end
    local names, marker = {}, false
    local w = assert(obs:watcher(function(info) if info then names[#names + 1] = info.name else marker = true end end))
    assert(w:wait_initial(5)); eq(marker, true)
    local before = #names
    obs:put("via-watcher", "1")
    assert(pump(3, function() return #names > before end), "update via watcher")
    eq(names[#names], "via-watcher")
    w:stop()
end)

-- cleanup -------------------------------------------------------------------
for i = #cleanup, 1, -1 do pcall(cleanup[i]) end
test("cleanup: no test streams left", function()
    if not js_ok then error("SKIP " .. js_why, 0) end
    local names = assert(nc:stream_names())
    for _, n in ipairs(names) do
        assert(not n:find(RID, 1, true), "leftover stream " .. n)
    end
end)
nc:close()

print(string.format("%s [json=%s]: %d passed, %d failed, %d skipped (prefix %s)",
    jit and jit.version or _VERSION, nats.json.backend, passed, failed, skipped, P))
os.exit(failed == 0 and 0 or 1)
