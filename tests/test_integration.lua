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
    eq(kv:get("a.b").op, "DEL")
    assert(kv:purge("new"))
    eq(kv:get("new").op, "PURGE")
    local st = kv:status()
    eq(st.history, 5)
    local seen
    local wsid = kv:watch_all(function(m) seen = m.subject end)
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
