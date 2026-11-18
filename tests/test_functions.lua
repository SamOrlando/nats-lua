-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Proves the client against a nats-server this file starts on 127.0.0.1:
--   14422  client (JetStream, no cluster, no auth)
--   18422  monitor
--   18480  websocket (no TLS)
--   14423  TLS client
-- usage: NUID_DIR=/path/to/lua-nuid lua tests/test_functions.lua
package.path = "./?.lua;./tests/?.lua;" .. package.path

local nats = require("nats")
nats.use("jetstream", "kvs", "objs", "micro")
local nuid = require("nuid")
local socket = require("socket")

local BIN = os.getenv("NATS_SERVER_BIN") or "nats-server"
local PORT, MONITOR, WSPORT, TLSPORT = 14422, 18422, 18480, 14423
local URL = "nats://127.0.0.1:" .. PORT
local RID = nuid.next():sub(1, 12)
local S = "fn." .. RID

local passed, failed = 0, 0
local nc
local owned, extras, svcs = {}, {}, {}
local DIR, procs, stopped = nil, {}, false

local function sh(cmd)
    local r = os.execute(cmd)
    return r == true or r == 0
end

local function why(err)
    if type(err) == "table" then
        local d = err.data
        if type(d) == "table" and type(d.error) == "table" then
            return d.error.description or nats.json.encode(d.error)
        end
        local ok, s = pcall(nats.json.encode, err)
        if ok then return s end
    end
    return tostring(err)
end

local function must(v, err, what)
    if v == nil or v == false then
        error((what and (what .. ": ") or "") .. why(err), 2)
    end
    return v
end

local function eq(a, b, msg)
    if a ~= b then
        error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2)
    end
end

local function has(list, val)
    if type(list) ~= "table" then return false end
    for _, v in ipairs(list) do if v == val then return true end end
    return false
end

local nseq = 0
local function subj(tag)
    nseq = nseq + 1
    return S .. "." .. (tag or "t") .. "." .. nseq
end

local function nm(tag)
    nseq = nseq + 1
    return "fn" .. RID .. (tag or "s") .. nseq
end

local function own(name) owned[#owned + 1] = name end
local function own_client(c) extras[#extras + 1] = c return c end
local function own_svc(s) svcs[#svcs + 1] = s return s end

local function drop_owned()
    if nc then
        for i = #owned, 1, -1 do
            pcall(nc.delete_stream, nc, owned[i])
            owned[i] = nil
        end
    else
        for i = #owned, 1, -1 do owned[i] = nil end
    end
    for i = #svcs, 1, -1 do
        pcall(function() svcs[i]:stop() end)
        svcs[i] = nil
    end
    for i = #extras, 1, -1 do
        pcall(function() extras[i]:close() end)
        extras[i] = nil
    end
end

local function test(name, fn)
    local ok, err = pcall(fn)
    pcall(drop_owned)
    if ok then
        passed = passed + 1
        print("ok   " .. name)
    else
        failed = failed + 1
        print("FAIL " .. name .. ": " .. tostring(err))
    end
end

local function connect(extra)
    local o = {
        servers = { URL },
        name = "nats-lua-fn",
        timeout = 3,
        ignore_discovered_servers = true,
        no_randomize = true,
    }
    for k, v in pairs(extra or {}) do o[k] = v end
    local c = nats.client(o)
    local ok, err = pcall(function() assert(c:connect()) end)
    if not ok then return nil, err end
    return c
end

local function pump(secs, done, other)
    local t = socket.gettime() + secs
    while socket.gettime() < t do
        nc:wait({ timeout = 0.05 })
        if other then other:wait({ timeout = 0.02 }) end
        if done and done() then return true end
    end
    return done == nil
end

local function busy(port)
    local s = socket.tcp()
    s:settimeout(0.2)
    local ok = s:connect("127.0.0.1", port)
    s:close()
    return ok and true or false
end

local function wait_port(port)
    for _ = 1, 100 do
        if busy(port) then return true end
        socket.sleep(0.05)
    end
    error("server on 127.0.0.1:" .. port .. " did not start")
end

local function http_get(port, path)
    local s = assert(socket.tcp())
    s:settimeout(2)
    assert(s:connect("127.0.0.1", port))
    assert(s:send("GET " .. path .. " HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"))
    local chunks = {}
    while true do
        local d, _, partial = s:receive(2048)
        if d then chunks[#chunks + 1] = d
        else
            if partial and #partial > 0 then chunks[#chunks + 1] = partial end
            break
        end
    end
    s:close()
    return table.concat(chunks)
end

local function kill_pidfile(pidfile)
    local f = io.open(pidfile)
    if not f then return end
    local pid = f:read("*l")
    f:close()
    if pid and pid:match("^%d+$") then sh("kill -9 " .. pid .. " >/dev/null 2>&1") end
end

local function stop_servers()
    if stopped then return end
    stopped = true
    for _, pidfile in ipairs(procs) do kill_pidfile(pidfile) end
    for _ = 1, 60 do
        if not busy(PORT) and not busy(MONITOR) and not busy(WSPORT) and not busy(TLSPORT) then break end
        socket.sleep(0.05)
    end
    if failed == 0 and not os.getenv("KEEP_LOGS") then
        if DIR then sh("rm -rf " .. DIR) end
    elseif DIR then
        print("logs in " .. DIR)
    end
end

local function start(name, conf, port)
    local path = DIR .. "/" .. name .. ".conf"
    local f = assert(io.open(path, "w"))
    f:write(conf)
    f:close()
    local pid = DIR .. "/" .. name .. ".pid"
    local log = DIR .. "/" .. name .. ".log"
    assert(sh(BIN .. " -c " .. path .. " -P " .. pid .. " > " .. log .. " 2>&1 &"), "launch " .. name)
    procs[#procs + 1] = pid
    local ok, err = pcall(wait_port, port)
    if not ok then
        local lf = io.open(log)
        local body = lf and lf:read("*a") or ""
        if lf then lf:close() end
        error(tostring(err) .. "\n" .. body)
    end
end

local function make_stream(tag, extra)
    local name = nm(tag)
    local cfg = { name = name, subjects = { S .. "." .. tag .. ".>" }, storage = "memory" }
    for k, v in pairs(extra or {}) do cfg[k] = v end
    local st, err = nc:create_stream(cfg)
    if not st then error(why(err), 2) end
    own(name)
    return st, S .. "." .. tag
end

local function run_tests()
    if not sh(BIN .. " --version >/dev/null 2>&1") then
        print("SKIP: nats-server not found (set NATS_SERVER_BIN)")
        os.exit(0)
    end
    for _, p in ipairs({ PORT, MONITOR, WSPORT, TLSPORT }) do
        if busy(p) then error("127.0.0.1:" .. p .. " is already in use") end
    end
    DIR = os.tmpname()
    os.remove(DIR)
    assert(sh("mkdir -p " .. DIR .. "/js"))
    local cert, key = DIR .. "/cert.pem", DIR .. "/key.pem"
    assert(sh("openssl req -x509 -newkey rsa:2048 -keyout " .. key .. " -out " .. cert ..
        " -days 1 -nodes -subj /CN=127.0.0.1 >/dev/null 2>&1"), "openssl cert")
    start("js", string.format([[
listen: 127.0.0.1:%d
http: 127.0.0.1:%d
server_name: nats-lua-fn
websocket { port: %d, no_tls: true }
jetstream { store_dir: %q }
]], PORT, MONITOR, WSPORT, DIR .. "/js"), PORT)
    wait_port(MONITOR)
    wait_port(WSPORT)
    start("tls", string.format([[
listen: 127.0.0.1:%d
tls { cert_file: %q, key_file: %q }
]], TLSPORT, cert, key), TLSPORT)

    test("handshake, account info, monitor, socket_factory", function()
        nc = must(connect())
        local si = nc.server_information
        assert(si.server_id and si.server_id ~= "", "INFO server_id")
        assert(si.headers, "headers")
        assert(si.proto and si.proto >= 1, "proto")
        assert(si.jetstream, "jetstream")
        eq(si.host or nc.current_server.host, "127.0.0.1")
        eq(nc.current_server.port, PORT)
        eq(nc:status_string(), "connected")
        eq(nc:is_connected(), true)
        eq(nc:is_reconnecting(), false)
        eq(nc:buffered(), 0)
        assert(nc:flush(2))
        assert(nc:ping(2))
        nc:pong()
        assert(nc:flush(2))
        local info
        local err
        for _ = 1, 40 do
            info, err = nc:account_info()
            if info then break end
            socket.sleep(0.1)
        end
        assert(info, why(err))
        assert(type(info.memory) == "number" or type(info.streams) == "number", "account info")
        local health = http_get(MONITOR, "/healthz")
        assert(health:find("ok", 1, true), "healthz")
        local connz = http_get(MONITOR, "/connz?limit=1024")
        assert(connz:find("nats-lua-fn", 1, true), "connz name")
        assert(connz:find('"lang": "lua"', 1, true) or connz:find('"lang":"lua"', 1, true), "connz lang")
        local raw = must(nats.socket_factory({ host = "127.0.0.1", port = PORT }, { timeout = 2 }))
        raw:close()
        local wire = nats.msg_string(nc, S .. ".wire", "hi")
        assert(wire:find("^PUB " .. S .. ".wire ", 1))
        eq(type(nats.msg_stringtable(S .. ".wire", "hi")[1]), "string")
        local rest = nats.split_header("NATS/1.0\r\nX-A: 1\r\n\r\n")
        assert(type(rest) == "string")
        print("     nats-server v" .. tostring(si.version) .. " jetstream=true")
    end)
    if not nc or not nc:is_connected() then return end

    test("module helpers and context connect", function()
        eq(nats.int("4"), 4)
        assert(type(nats.context_dir()) == "string")
        assert(nats.timestamp(0):find("^1970%-01%-01T"))
        eq(nats.parse_url("nats://127.0.0.1:" .. PORT).port, PORT)
        eq(nats.valid_subject("a.b"), "a.b")
        assert(nats.valid_publish_subject("a.b"))
        assert(nats.valid_queue("workers"))
        eq(nats.valid_stream_name("fnok"), "fnok")
        eq(nats.valid_stream_name("a.b"), nil)
        eq(nats.valid_consumer_name("c1"), "c1")
        assert(nats.valid_bucket_name("buck"))
        assert(nats.valid_key_name("a.b"))
        assert(nats.valid_object_store_name("obj"))
        eq(nats.json.decode(nats.json.encode({ a = 1 })).a, 1)
        local ok, err = pcall(nats.error, "boom", 0)
        assert(not ok and tostring(err):find("boom", 1, true))
        eq(nats.jetstream.ttl_string(1.2), "2")
        eq(nats.jetstream.incr_string(5), "+5")
        eq(nats.jetstream.b64_decode("aGk="), "hi")
        eq(nats.jetstream.api_prefix(), "$JS.API.")
        eq(nats.jetstream.api_prefix({ domain = "D" }), "$JS.D.API.")
        local req = nats.jetstream.priority_fields({ pin_id = "p" }, {}, { group = "g", id = "x" })
        eq(req.group, "g")
        eq(req.id, "x")
        local sc = nats.kvs.stream_config({ bucket = "abc", history = 2, storage = "memory" })
        eq(sc.name, "KV_abc")
        local oc = nats.objs.stream_config({ bucket = "abc", storage = "memory" })
        eq(oc.name, "OBJ_abc")
        eq(nats.objs.b64url_decode(nats.objs.b64url_encode("hi")), "hi")
        local parts = {}
        for chunk in nats.objs.chunker("abcdef", 2) do parts[#parts + 1] = chunk end
        -- chunker may be an iterator or a list; accept either shape
        if #parts == 0 and type(nats.objs.chunker("abcdef", 2)) == "table" then
            parts = nats.objs.chunker("abcdef", 2)
        end
        assert(#parts >= 1, "chunker")
        assert(nats.micro.valid_name("svc"))
        eq(nats.micro.valid_version("1.2.3"), true)
        eq(nats.micro.valid_version("01.2.3"), false)
        local eh = nats.micro.error_headers(422, "nope")
        assert(eh["Nats-Service-Error-Code"] or next(eh))
        assert(type(nats.micro.encode_body({ ok = true })) == "string")
        local frame = nats.ws_frame(0x2, "hi", false)
        assert(type(frame) == "string" and #frame >= 4)
        local path = DIR .. "/ctx.json"
        local f = assert(io.open(path, "w"))
        f:write('{"url":"' .. URL .. '"}\n')
        f:close()
        local opts = must(nats.load_context(path))
        eq(opts.servers, URL)
        local c = own_client(must(nats.connect_context(path, {
            ignore_discovered_servers = true, no_randomize = true, timeout = 3, name = "nats-lua-fn",
        })))
        assert(c:is_connected())
        assert(c:flush(2))
        local plain = own_client(nats.connect("127.0.0.1", PORT))
        assert(plain:flush(2))
        eq(plain.current_server.host, "127.0.0.1")
        eq(plain.current_server.port, PORT)
    end)

    test("pub/sub, headers, queue, unsubscribe, max payload", function()
        local subject = subj("ps")
        local got
        local sid = nc:subscribe(subject, function(m) got = m end)
        nc:flush()
        local payload = "a\r\nb\0\255"
        nc:publish(subject, payload, nil, { ["X-One"] = "1", ["X-Multi"] = { "a", "b" } })
        nc:wait({ sid = sid, timeout = 2 })
        assert(got, "message")
        eq(got.data, payload)
        local h = got:headers()
        eq(h["X-One"], "1")
        eq(h["X-Multi"][1], "a")
        eq(h["X-Multi"][2], "b")
        eq(got:status(), nil)
        local bad_ack, bad_err = got:ack()
        eq(bad_ack, false)
        eq(bad_err, "ErrMsgNoReply")
        nc:unsubscribe(sid)
        local c2 = own_client(must(connect()))
        local q1, q2, w = 0, 0, 0
        local qs = subj("q")
        nc:subscribe(S .. ".wc.*.x", function() w = w + 1 end)
        nc:subscribe(qs, function() q1 = q1 + 1 end, "workers")
        c2:subscribe(qs, function() q2 = q2 + 1 end, "workers")
        nc:flush(); c2:flush()
        nc:publish(S .. ".wc.a.x", "1")
        nc:publish(S .. ".wc.b.x", "2")
        nc:publish(S .. ".wc.b.y", "no")
        for i = 1, 10 do nc:publish(qs, tostring(i)) end
        nc:flush()
        pump(1, function() return w == 2 and q1 + q2 == 10 end, c2)
        eq(w, 2, "wildcard")
        eq(q1 + q2, 10, "queue")
        local n = 0
        local ms = subj("max")
        local msid = nc:subscribe(ms, function() n = n + 1 end)
        nc:unsubscribe(msid, 2)
        for _ = 1, 6 do nc:publish(ms, "x") end
        nc:flush()
        nc:wait({ timeout = 0.4 })
        eq(n, 2)
        eq(nc.subscriptions[msid], nil)
        local maxp = nc.server_information.max_payload
        local ok, err = pcall(nc.publish, nc, subj("big"), string.rep("x", maxp + 1))
        assert(not ok and tostring(err):find("maximum payload"), tostring(err))
        local bad, berr = pcall(nc.publish, nc, "bad subject", "x")
        assert(not bad and tostring(berr):find("invalid subject"), tostring(berr))
        assert(nc:flush(2))
    end)

    test("request, get, poll, request_many", function()
        local echo = subj("echo")
        local sid = nc:subscribe(echo, function(m) m:respond("re:" .. m.data) end)
        nc:flush()
        local r = must(nc:get(echo, "ping"))
        eq(r.data, "re:ping")
        local rs = must(nc:request_sync(echo, "sync"))
        eq(rs.data, "re:sync")
        local got
        local req = nc:request(echo, function(m) got = m.data end, "cb")
        local deadline = socket.gettime() + 2
        while not got and socket.gettime() < deadline do nc:wait({ lid = req.lid, timeout = 0.05 }) end
        eq(got, "re:cb")
        local polls = 0
        local many = subj("poll")
        nc:subscribe(many, function(m)
            nc:publish(m.reply, "a")
            nc:publish(m.reply, "b")
        end)
        nc:flush()
        local pm = nc:poll(many, function() polls = polls + 1 end, "x", nil, 1, 2)
        deadline = socket.gettime() + 2
        while polls < 2 and socket.gettime() < deadline do nc:wait({ timeout = 0.05 }) end
        eq(polls, 2)
        eq(pm.lid ~= nil, true)
        local rm = subj("rm")
        nc:subscribe(rm, function(m) m:respond("1") end)
        nc:subscribe(rm, function(m) m:respond("2") end)
        nc:flush()
        local res = nc:request_many(rm, "x", { max_wait = 1, stall = 0.3 })
        eq(#res, 2)
        local ncb = 0
        nc:request_many(rm, "x", { max_wait = 1, max_messages = 1, callback = function()
            ncb = ncb + 1
            return false
        end })
        eq(ncb, 1)
        local sent = subj("sent")
        nc:subscribe(sent, function(m)
            m:respond("1")
            m:respond("")
        end)
        nc:flush()
        local only = nc:request_many(sent, "", { max_wait = 1, sentinel = true })
        eq(#only, 1)
        eq(only[1].data, "1")
        local nobody, nerr, raw = nc:get(subj("nobody"), "x", nil, nil, 1)
        eq(nobody, nil)
        eq(nerr, "no responders")
        eq(raw:status(), nats.status.NO_RESPONDERS)
        nc:unsubscribe(sid)
    end)

    test("message methods", function()
        local subject = subj("msg")
        local got
        nc:subscribe(subject, function(m) got = m.data end)
        local reply_to = subj("msgrep")
        nc:subscribe(reply_to, function(m) m:respond("pong:" .. m.data) end)
        nc:flush()
        local m = nc:msg(subject, "body", reply_to)
        eq(m:size(), #subject + #reply_to + #"body")
        local cloned = m:clone()
        eq(cloned.data, "body")
        eq(cloned.subject, subject)
        local made = m:new(reply_to, "q")
        eq(made.subject, reply_to)
        eq(made.data, "q")
        m:publish()
        nc:wait({ timeout = 1 })
        eq(got, "body")
        local gr = must(made:get())
        eq(gr.data, "pong:q")
        local rg
        made:request(nil, function(x) rg = x.data end)
        local deadline = socket.gettime() + 2
        while not rg and socket.gettime() < deadline do nc:wait({ timeout = 0.05 }) end
        eq(rg, "pong:q")
    end)

    test("inbox, tickers, pool, auth", function()
        local ib, lid = nc:create_inbox()
        assert(ib:sub(1, #nc.inbox_muxed) == nc.inbox_muxed)
        eq(#lid, 8)
        eq(nc.inbox_sid, "1")
        eq(nc:inbox_subscribe(), "1")
        local fresh = own_client(must(connect()))
        eq(fresh.inbox_sid, "1")
        eq(fresh:subscribe(subj("sid"), function() end), "2")
        local custom = nc:new_inbox("lab.ibx")
        assert(custom:find("^lab%.ibx%.[%w]+$"))
        local one = nats.new_inbox("lab.ibx")
        assert(one:find("^lab%.ibx%.[%w]+$"))
        eq(select("#", nats.new_inbox()), 1)
        local n = 0
        nc:add_ticker(0.01, function()
            n = n + 1
            if n >= 3 then return false end
        end)
        nc:wait({ timeout = 0.3 })
        eq(n, 3)
        local z = 0
        local t = nc:add_ticker(10, function() z = z + 1 end)
        nc:remove_ticker(t)
        nc:wait({ timeout = 0.05 })
        eq(z, 0)
        local pool = nc:set_server_pool(URL)
        eq(pool[1].host, "127.0.0.1")
        eq(pool[1].port, PORT)
        assert(nc:is_connected())
        local c = own_client(nats.client({ servers = { URL }, timeout = 2 }))
        local a = c:auth("alice", "secret")
        eq(a.user, "alice")
        eq(a.pass, "secret")
        eq(c:is_connected(), false)
    end)

    test("encoded client", function()
        local ec = nc:encoded_client()
        eq(ec:decode_data(ec:encode_data({ z = 1 })).z, 1)
        eq(ec:decode_header(ec:encode_header({ ["X-A"] = "1" }))["X-A"], "1")
        local subject = subj("enc")
        nc:subscribe(subject, function(m)
            local d = nats.json.decode(m.data)
            m:respond(nats.json.encode({ sum = d.a + d.b }))
        end)
        nc:flush()
        local r = must(ec:get(subject, { a = 2, b = 3 }))
        eq(r.data.sum, 5)
        local got
        ec:request(subject, function(m) got = m.data.sum end, { a = 1, b = 4 })
        local deadline = socket.gettime() + 2
        while got == nil and socket.gettime() < deadline do nc:wait({ timeout = 0.05 }) end
        eq(got, 5)
        local ps = subj("encps")
        local seen
        local sid = ec:subscribe(ps, function(m) seen = m.data.k end)
        nc:flush()
        ec:publish(ps, { k = 7 })
        nc:wait({ sid = sid, timeout = 2 })
        eq(seen, 7)
        local polls = 0
        local pr = subj("encpoll")
        nc:subscribe(pr, function(m) m:respond(nats.json.encode({ n = 1 })) end)
        nc:flush()
        ec:poll(pr, function(m) polls = polls + (m.data.n or 0) end, { q = 1 }, nil, 1, 1)
        deadline = socket.gettime() + 2
        while polls < 1 and socket.gettime() < deadline do nc:wait({ timeout = 0.05 }) end
        eq(polls, 1)
        nc:unsubscribe(sid)
    end)

    test("reconnect hooks", function()
        local subject = subj("rc")
        local n, hooks = 0, 0
        local c = own_client(must(connect({ reconnect = true, reconnect_wait = 0.05, reconnect_jitter = 0 })))
        local function hook() hooks = hooks + 1 end
        c:on_reconnect(hook)
        local sid = c:subscribe(subject, function() n = n + 1 end)
        c:flush()
        assert(c:reconnect())
        eq(c.stats.reconnects, 1)
        eq(hooks, 1)
        eq(c:status_string(), "connected")
        eq(c:is_reconnecting(), false)
        c:off_reconnect(hook)
        assert(c:reconnect())
        eq(c.stats.reconnects, 2)
        eq(hooks, 1)
        -- SUB is written before reconnect returns. Flush so the server has it
        -- before another connection publishes (LuaJIT otherwise wins that race).
        assert(c:flush())
        nc:publish(subject, "after")
        nc:flush()
        local deadline = socket.gettime() + 2
        while n < 1 and socket.gettime() < deadline do c:wait({ sid = sid, timeout = 0.05 }) end
        eq(n, 1, "resubscribed")
    end)

    test("drain_sub, drain, shutdown", function()
        local subject = subj("dsub")
        local c = own_client(must(connect()))
        local n = 0
        local sid = c:subscribe(subject, function() n = n + 1 end)
        c:flush()
        nc:publish(subject, "x")
        nc:flush()
        assert(c:drain_sub(sid, 2))
        eq(n, 1)
        eq(c.subscriptions[sid], nil)
        assert(c:is_connected())
        local subject2 = subj("drain")
        local n2 = 0
        local c2 = own_client(must(connect()))
        c2:subscribe(subject2, function() n2 = n2 + 1 end)
        c2:flush()
        for _ = 1, 5 do nc:publish(subject2, "x") end
        nc:flush()
        socket.sleep(0.15)
        assert(c2:drain(3))
        eq(n2, 5)
        eq(c2.closed, true)
        eq(c2:status_string(), "closed")
        local c3 = own_client(must(connect()))
        assert(c3:flush(2))
        c3:shutdown()
        eq(c3.closed, true)
        eq(c3:is_connected(), false)
    end)

    test("websocket and tls", function()
        local ws = own_client(must(connect({ servers = { "ws://127.0.0.1:" .. WSPORT } })))
        local subject = subj("ws")
        local got
        local sid = ws:subscribe(subject, function(m) got = m.data end)
        ws:flush()
        nc:publish(subject, "from-tcp")
        nc:flush()
        ws:wait({ sid = sid, timeout = 2 })
        eq(got, "from-tcp")
        local upper = subj("wsup")
        ws:subscribe(upper, function(m) m:respond((m.data or ""):upper()) end)
        ws:flush()
        local r = must(ws:get(upper, "abc"))
        eq(r.data, "ABC")
        local cross = subj("wsc")
        local crossed
        nc:subscribe(cross, function(m) m:respond("tcp:" .. m.data) end)
        nc:flush()
        local req = ws:request(cross, function(m) crossed = m.data end, "z")
        local deadline = socket.gettime() + 2
        while not crossed and socket.gettime() < deadline do
            nc:wait({ timeout = 0.05 })
            ws:wait({ lid = req.lid, timeout = 0.05 })
        end
        eq(crossed, "tcp:z")
        local tc = own_client(must(connect({
            servers = { "tls://127.0.0.1:" .. TLSPORT },
            tls = { verify = "none" },
        })))
        local tsj = subj("tls")
        local seen
        local tsid = tc:subscribe(tsj, function(m) seen = m.data end)
        tc:flush()
        tc:publish(tsj, "secure")
        tc:wait({ sid = tsid, timeout = 2 })
        eq(seen, "secure")
        assert(tc:flush(2))
    end)

    test("jetstream api, stream create, update, list, delete", function()
        local m = must(nats.jetstream.api(nc, "$JS.API.INFO"))
        assert(type(m.data) == "table")
        local name = nm("crud")
        local base = S .. ".crud"
        local st = must(nc:create_stream({ name = name, subjects = { base .. ".>" }, storage = "memory" }))
        own(name)
        eq(st.name, name)
        local info = must(nc:get_stream(name))
        eq(info.config.subjects[1], base .. ".>")
        local updated = must(st:update({
            name = name, subjects = { base .. ".>" }, storage = "memory", max_msgs = 50, description = "updated",
        }))
        eq(updated.config.max_msgs, 50)
        eq(updated.config.description, "updated")
        local again = must(nc:update_stream({
            name = name, subjects = { base .. ".>" }, storage = "memory", max_msgs = 40, description = "updated",
        }))
        eq(again.config.max_msgs, 40)
        local cu_name = nm("cu")
        local cu = must(nc:create_update_stream({
            name = cu_name, subjects = { S .. ".cu.>" }, storage = "memory",
        }))
        own(cu_name)
        local cu2 = must(nc:create_update_stream({
            name = cu_name, subjects = { S .. ".cu.>" }, storage = "memory", max_msgs = 9,
        }))
        eq(cu2.config.max_msgs, 9)
        eq(nc:stream_by_subject(base .. ".a"), name)
        local names = must(nc:stream_names(base .. ".>"))
        assert(has(names, name))
        local paged = must(nats.jetstream.paged(nc, "$JS.API.STREAM.NAMES", "streams", { subject = base .. ".>" }))
        assert(has(paged, name))
        local list = must(nc:list_streams(base .. ".>"))
        assert(list.total >= 1)
        local found = false
        for _, item in ipairs(list.streams) do
            local n = item.name or (item.config and item.config.name)
            if n == name then found = true end
        end
        assert(found, "list_streams")
        assert(cu:delete())
        eq(nc:get_stream(cu_name), nil)
        assert(nc:delete_stream(name))
        for i = #owned, 1, -1 do if owned[i] == name or owned[i] == cu_name then table.remove(owned, i) end end
    end)

    test("publish, get, purge, delete message", function()
        local st, base = make_stream("pub", { allow_direct = true })
        local a = must(st:publish(base .. ".a", "a1", { id = "m1" }))
        eq(a.seq, 1)
        eq(a.stream, st.name)
        local dup = must(st:publish(base .. ".a", "a1", { id = "m1" }))
        eq(dup.duplicate, true)
        local b = must(nc:js_publish(base .. ".b", "b1"))
        eq(b.seq, 2)
        local via = must(nats.jetstream.publish(nc, base .. ".a", "a2", { stream = st.name, last_seq = b.seq }))
        eq(via.seq, 3)
        local bad, berr = nc:js_publish(base .. ".a", "no", { stream = "other-stream" })
        eq(bad, nil)
        assert(berr, "expected stream mismatch")
        local msg = nc:msg(base .. ".b", "b2")
        local pb = must(st:publish(msg, { header = { ["X-A"] = "1" }, last_msg_id = nil }))
        eq(pb.seq, 4)
        local direct = must(st:getmsg(base .. ".a"))
        eq(direct.data, "a2")
        eq(must(st:lastmsg(base .. ".a")).data, "a2")
        eq(must(st:nextmsg(base .. ".a", 1)).data, "a1")
        eq(must(st:getmsg(1)).data, "a1")
        local stored = must(st:get_stored(4))
        eq(stored.data, "b2")
        eq(stored:headers()["X-A"], "1")
        eq(stored:headers()["Nats-Sequence"], "4")
        local plain, pbase = make_stream("gst")
        must(plain:publish(pbase .. ".a", "pa1"))
        must(plain:publish(pbase .. ".b", "pb1"))
        must(plain:publish(pbase .. ".a", "pa2", { header = { ["X-B"] = "2" } }))
        local gs = must(plain:get_stored(pbase .. ".a"))
        eq(gs.data, "pa2")
        eq(gs:headers()["X-B"], "2")
        eq(gs:headers()["Nats-Subject"], pbase .. ".a")
        assert(plain:deletemsg(1))
        assert(plain:deletemsg(2, true))
        local gone, gerr = plain:get_stored(1)
        eq(gone, nil)
        assert(tostring(gerr):find("not found"), tostring(gerr))
        gone, gerr = plain:get_stored(2)
        eq(gone, nil)
        assert(tostring(gerr):find("not found"), tostring(gerr))
        eq(must(plain:get_stored(3)).data, "pa2")
        assert(st:purge(base .. ".b"))
        local left = must(st:info())
        eq(left.state.messages, 2)
        assert(st:purge(nil, nil, 1))
        eq(must(st:info()).state.messages, 1)
    end)

    test("consumer crud, fetch, ack, nak, term, in_progress, ack_next", function()
        local st, base = make_stream("con")
        local sub = base .. ".m"
        local c = must(st:create_consumer({ name = "c1", durable_name = "c1", ack_policy = "explicit" }))
        eq(c.name, "c1")
        local upd = must(c:update({ name = "c1", durable_name = "c1", ack_policy = "explicit", description = "d1" }))
        eq(must(upd:info()).config.description, "d1")
        local cu = must(st:create_update_consumer({ name = "c1", durable_name = "c1", ack_policy = "explicit", description = "d2" }))
        eq(must(cu:info()).config.description, "d2")
        eq(must(st:get_consumer("c1")).name, "c1")
        local listed = must(st:consumer_list())
        assert(listed.total >= 1)
        local names = must(st:consumer_names())
        assert(has(names, "c1"))
        must(st:publish(sub, "a"))
        local m = must(c:next(2))
        eq(m.data, "a")
        local meta = m:metafields()
        eq(meta.stream, st.name)
        eq(meta.consumer, "c1")
        eq(meta.stream_seq, 1)
        local pm, pb = m:pending()
        assert(pm == nil or type(pm) == "number")
        assert(pb == nil or type(pb) == "number")
        assert(type(meta.pending) == "number" or meta.pending == nil)
        assert(m:ack())
        eq(m:acked(), true)
        local again, aerr = m:ack()
        eq(again, false)
        assert(tostring(aerr):find("already"), tostring(aerr))
        must(st:publish(sub, "b"))
        m = must(c:next(2))
        eq(m.data, "b")
        assert(m:nak())
        local re = must(c:next(2))
        eq(re.data, "b")
        eq(re:metafields().delivered, 2)
        assert(re:ack_ack())
        eq(re:acked(), true)
        must(st:publish(sub, "c"))
        m = must(c:next(2))
        assert(m:double_ack())
        eq(m:acked(), true)
        must(st:publish(sub, "d"))
        m = must(c:next(2))
        eq(m.data, "d")
        assert(m:term("bye"))
        eq(m:acked(), true)
        local none = c:fetch(1)
        eq(#none, 0)
        must(st:publish(sub, "e"))
        m = must(c:next(2))
        assert(m:in_progress())
        assert(not m:acked())
        assert(m:wpi())
        assert(not m:acked())
        assert(m:ack())
        eq(m:acked(), true)
        must(st:publish(sub, "f"))
        m = must(c:next(2))
        local saved = nc.config.timeout
        nc.config.timeout = 0.3
        local reply, nerr = m:ack_next()
        nc.config.timeout = saved
        eq(m:acked(), true)
        if reply == nil then eq(nerr, "timeout") end
        eq(must(c:info()).num_ack_pending, 0)
        assert(c:delete())
        local other = must(st:consumer({ name = "c2", ack_policy = "explicit" }, "create"))
        assert(st:delete_consumer(other.name))
        eq(st:get_consumer("c2"), nil)
    end)

    test("consume, messages, stop, drain", function()
        local st, base = make_stream("pull")
        for i = 1, 3 do must(nc:js_publish(base .. ".x", "n" .. i)) end
        local c = must(st:create_consumer({ name = "work", ack_policy = "explicit" }))
        local n = 0
        local ctx = must(c:consume(function(msg) n = n + 1 msg:ack() end, { max_messages = 2, expires = 2 }))
        assert(pump(6, function() return n == 3 end), "consumed " .. n)
        eq(ctx:closed(), false)
        local ok, nctx = c:drain(3, 0.2)
        assert(ok)
        assert(nctx >= 1)
        eq(ctx:closed(), true)
        local c2 = must(st:create_consumer({ name = "iter", ack_policy = "explicit", deliver_policy = "new" }))
        local it = must(c2:messages({ max_messages = 2, expires = 2 }))
        must(nc:js_publish(base .. ".x", "late"))
        local m = it:next(4)
        assert(m and m.data == "late", "iterator")
        m:ack()
        it:stop()
        eq(it.ctx:closed(), true)
        local c3 = must(st:create_consumer({ name = "stp", ack_policy = "explicit", deliver_policy = "new" }))
        local n3 = 0
        c3:consume(function() n3 = n3 + 1 end, { max_messages = 2, expires = 2 })
        must(nc:js_publish(base .. ".x", "s"))
        assert(pump(4, function() return n3 >= 1 end), "stop saw " .. n3)
        eq(c3:stop(), 1)
    end)

    test("pause, resume, pin, unpin", function()
        local st, base = make_stream("pause")
        local c = must(st:create_consumer({ name = "pausable", ack_policy = "explicit" }))
        eq(must(st:pause_consumer(c.name, os.time() + 90)).paused, true)
        assert(pump(3, function() return c:info().paused == true end), "paused info")
        eq(must(st:resume_consumer(c.name)).paused, false)
        eq(must(c:pause(os.time() + 90)).paused, true)
        eq(must(c:resume()).paused, false)
        for i = 1, 3 do must(nc:js_publish(base .. ".p", "p" .. i)) end
        local pc, perr = st:consumer({
            name = "pinned", ack_policy = "explicit", priority_groups = { "g1" },
            priority_policy = "pinned_client", priority_timeout = 30 * nats.jetstream.NS,
        }, "create")
        assert(pc, why(perr))
        local msgs = pc:fetch(1, nil, 2, nil, { group = "g1" })
        eq(#msgs, 1)
        assert(pc.pin_id, "pin id")
        msgs[1]:ack()
        assert(pc:unpin("g1"))
        local more, merr = pc:fetch(1, nil, 2, nil, { group = "g1" })
        if #more == 0 then more, merr = pc:fetch(1, nil, 2, nil, { group = "g1" }) end
        if #more ~= 1 then error("pinned fetch got " .. #more .. " " .. tostring(merr)) end
        more[1]:ack()
        assert(st:unpin_consumer(pc.name, "g1"))
    end)

    test("ordered consumer", function()
        local st, base = make_stream("ord")
        for i = 1, 3 do must(nc:js_publish(base .. ".a", "a" .. i)) end
        must(nc:js_publish(base .. ".b", "b1"))
        local oc = st:ordered_consumer({ filter_subject = base .. ".a" })
        local got, resets = 0, 0
        oc.cfg.reset_cb = function() resets = resets + 1 end
        local ctx = must(oc:consume(function() got = got + 1 end, { idle_heartbeat = 1, expires = 2 }))
        assert(pump(5, function() return got >= 3 end), "ordered " .. got)
        local info = must(oc:info())
        assert(info.name:find(oc.prefix, 1, true) == 1)
        st:delete_consumer(oc.consumer.name)
        must(nc:js_publish(base .. ".a", "a4"))
        assert(pump(8, function() return got >= 4 end), "after reset " .. got)
        assert(resets >= 1, "reset " .. resets)
        ctx.stop()
        local f = st:ordered_consumer({ filter_subject = base .. ".b" })
        eq(#f:fetch(10, 2), 1)
        local one = f:next(1)
        eq(one, nil)
        assert(f:info())
        f:stop()
        local neu = st:ordered_consumer({ filter_subject = base .. ".c", deliver_policy = "new" })
        local it = must(neu:messages({ max_messages = 2, expires = 2 }))
        must(nc:js_publish(base .. ".c", "late"))
        local m = it:next(4)
        assert(m and m.data == "late")
        assert(it:drain(2, 0.2))
        neu:stop()
    end)

    test("push subscribe, drain, ordered reset", function()
        local st, base = make_stream("push")
        for i = 1, 4 do must(nc:js_publish(base .. ".a", "p" .. i)) end
        local n, errs = 0, 0
        local sub = must(st:subscribe({ deliver_policy = "all", ack_policy = "none" }, function() n = n + 1 end, {
            flow_control = true, idle_heartbeat = 1, error_cb = function() errs = errs + 1 end,
        }))
        assert(pump(5, function() return n == 4 end), "push " .. n)
        eq(sub:closed(), false)
        local before = sub.last_activity
        pump(2.4)
        assert(sub.last_activity > before, "heartbeat")
        eq(errs, 0)
        assert(sub:drain(2))
        eq(sub:closed(), true)
        local n2 = 0
        local sub2 = must(st:subscribe(function() n2 = n2 + 1 end))
        assert(pump(4, function() return n2 == 4 end), "push2 " .. n2)
        sub2:unsubscribe()
        eq(sub2:closed(), true)
        local on = 0
        local osub = must(st:subscribe({ filter_subject = base .. ".a" }, function() on = on + 1 end, {
            ordered = true, idle_heartbeat = 1,
        }))
        assert(pump(5, function() return on == 4 end), "ordered push " .. on)
        osub:reset("test")
        osub:stop()
        eq(osub:closed(), true)
    end)

    test("direct get", function()
        local st, base = make_stream("dg", { allow_direct = true })
        for _, t in ipairs({ "a", "b", "c" }) do must(nc:js_publish(base .. "." .. t, t)) end
        must(nc:js_publish(base .. ".a", "a2"))
        local batch, eob = st:direct_get({ batch = 2, seq = 1 })
        if not batch then error(why(eob)) end
        eq(#batch, 2)
        assert(type(eob) == "table")
        local last, lerr = st:direct_get({ multi_last = { base .. ".*" } })
        if not last then error(why(lerr)) end
        eq(#last, 3)
    end)

    test("per message ttl", function()
        local st, base = make_stream("ttl", { allow_msg_ttl = true, allow_direct = true })
        must(nc:js_publish(base .. ".x", "gone", { ttl = 1 }))
        must(nc:js_publish(base .. ".y", "stays", { ttl = "never" }))
        assert(pump(8, function() return st:getmsg(base .. ".x") == nil end), "ttl expired")
        assert(st:getmsg(base .. ".y"), "never expires")
    end)

    test("counters and atomic batch", function()
        local empty, eerr = nc:js_publish_batch({})
        eq(empty, nil)
        eq(eerr, "empty batch")
        local cs, base = make_stream("cnt", { allow_msg_counter = true, allow_direct = true })
        local counter = base .. ".hits"
        eq(must(nc:js_counter_add(counter, 5)), 5)
        eq(must(cs:counter_add(counter, -2)), 3)
        eq(must(cs:counter_get(counter)), 3)
        local as, abase = make_stream("atom", { allow_atomic = true })
        local batch = {}
        for i = 1, 3 do batch[i] = { subject = abase .. ".b" .. i, data = "v" .. i } end
        local ack = must(nc:js_publish_batch(batch))
        eq(ack.count, 3)
        eq(must(as:info()).state.messages, 3)
    end)

    test("kv put get update delete purge history", function()
        local b = "fn" .. RID .. "kv"
        local kv = must(nc:create_key_value({ bucket = b, history = 5, storage = "memory", description = "orig" }))
        own("KV_" .. b)
        local kv2 = must(nc:update_key_value({ bucket = b, history = 5, storage = "memory", description = "upd" }))
        eq(kv2.stream.config.description, "upd")
        eq(kv2:status().bucket, b)
        eq(kv2:status().history, 5)
        eq(kv2:status().backing_store, "JetStream")
        local bound = must(nc:key_value(b))
        eq(must(kv:put("a.b", "v1")), 1)
        local e = must(kv:get("a.b"))
        eq(e.value, "v1")
        eq(e.revision, 1)
        eq(e.op, "PUT")
        local built = nats.kvs.entry_from(kv, "a.b", { data = "v1" }, {}, 1, e.created, 0)
        eq(built.op, "PUT")
        eq(built.value, "v1")
        eq(must(kv:update("a.b", "v2", 1)), 2)
        local bad, berr = kv:update("a.b", "v3", 1)
        eq(bad, nil)
        eq(berr, "wrong last sequence")
        eq(must(kv:get("a.b")).value, "v2")
        eq(must(kv:get_revision("a.b", 1)).value, "v1")
        eq(must(kv:create("new", "x")), 3)
        local c2, cerr = kv:create("new", "y")
        eq(c2, nil)
        eq(cerr, "key exists")
        assert(kv:delete("new"))
        eq(must(kv:create("new", "again")), 5)
        local miss, merr = kv:get("missing")
        eq(miss, nil)
        eq(merr, "key not found")
        assert(kv:purge("new"))
        local _, _, pe = kv:get("new")
        eq(pe.op, "PURGE")
        local names = must(nc:key_value_store_names())
        assert(has(names, b))
        eq(must(bound:get("a.b")).value, "v2")
        local h = must(kv:history("a.b"))
        eq(#h >= 2, true)
        eq(h[1].value, "v1")
        local ks = must(kv:keys())
        assert(has(ks, "a.b"))
        local seen = false
        for key in must(kv:list_keys()) do if key == "a.b" then seen = true end end
        assert(seen, "list_keys")
        local b2 = "fn" .. RID .. "kv2"
        local made = must(nc:kvs({ bucket = b2, history = 2, storage = "memory" }))
        own("KV_" .. b2)
        must(made:put("k", "1"))
        local merged = must(nc:create_or_update_key_value({ bucket = b2, history = 3, storage = "memory" }))
        eq(merged:status().history, 3)
        assert(nc:delete_key_value(b2))
        for i = #owned, 1, -1 do if owned[i] == "KV_" .. b2 then table.remove(owned, i) end end
        assert(kv:destroy())
        for i = #owned, 1, -1 do if owned[i] == "KV_" .. b then table.remove(owned, i) end end
    end)

    test("kv watch, purge_deletes, ttl, mirror, sources", function()
        local b = "fn" .. RID .. "kvw"
        local kv = must(nc:create_key_value({ bucket = b, history = 10, storage = "memory" }))
        own("KV_" .. b)
        kv:put("a.1", "x")
        kv:put("a.2", "y")
        kv:put("a.1", "x2")
        local seen, marker = {}, 0
        local w = must(kv:watch("a.*", function(ent)
            if ent then seen[#seen + 1] = ent.key .. "=" .. tostring(ent.value) else marker = marker + 1 end
        end))
        assert(w:wait_initial(5))
        eq(#seen, 2)
        eq(marker, 1)
        kv:put("a.3", "new")
        assert(pump(3, function() return #seen == 3 end), "watch update")
        eq(seen[3], "a.3=new")
        w:unsubscribe()
        local all = {}
        local w2 = must(kv:watch_all(function(ent) if ent then all[#all + 1] = ent.key end end))
        assert(w2:wait_initial(5))
        w2:stop()
        assert(has(all, "a.1") and has(all, "a.2"), "watch_all")
        kv:delete("a.3")
        kv:purge("a.2")
        eq(must(kv:purge_deletes({ older_than = 0 })), 2)
        local tb = "fn" .. RID .. "kvt"
        local tkv = must(nc:create_key_value({ bucket = tb, storage = "memory", limit_marker_ttl = 1 }))
        own("KV_" .. tb)
        eq(tkv:status().limit_marker_ttl, 1)
        assert(tkv:create("eph", "v", { ttl = 1 }))
        local ops = {}
        local tw = tkv:watch("eph", function(ent) if ent then ops[#ops + 1] = ent.op end end)
        assert(pump(8, function() return ops[#ops] == "PURGE" end), "marker " .. table.concat(ops, ","))
        tw:stop()
        local mb, sb = "fn" .. RID .. "kvm", "fn" .. RID .. "kvs"
        local mir = must(nc:create_key_value({ bucket = mb, storage = "memory", mirror = { name = tb } }))
        own("KV_" .. mb)
        local src = must(nc:create_key_value({ bucket = sb, storage = "memory", sources = { { name = tb } } }))
        own("KV_" .. sb)
        tkv:put("shared", "42")
        assert(pump(5, function()
            local ent = src:get("shared")
            return ent and ent.value == "42"
        end), "source")
        assert(pump(5, function()
            local ent = mir:get("shared")
            return ent and ent.value == "42"
        end), "mirror")
        eq(src.stream.config.sources[1].subject_transforms[1].dest, "$KV." .. sb .. ".>")
        assert(mir:put("viamirror", "1"))
        eq(tkv:get("viamirror").value, "1")
    end)

    test("object store put get file link seal", function()
        local b = "fn" .. RID .. "obj"
        local b2 = "fn" .. RID .. "obj2"
        local obs = must(nc:create_object_store({
            bucket = b, description = "nats-lua test", storage = "memory", metadata = { owner = "nats-lua" },
        }))
        own("OBJ_" .. b)
        local upd = must(nc:update_object_store({ bucket = b, description = "updated", storage = "memory" }))
        eq(upd:status().description, "updated")
        local obs2 = must(nc:objs({ bucket = b2, storage = "memory", description = "two" }))
        own("OBJ_" .. b2)
        eq(must(nc:object_store(b)).name, b)
        local names = must(nc:object_store_names())
        assert(has(names, b) and has(names, b2))
        local info = must(obs:put_string("hello.txt", "hello world"))
        eq(info.size, 11)
        eq(obs:get_string("hello.txt"), "hello world")
        eq(must(obs:put_bytes("empty", "")).chunks, 0)
        eq(obs:get_bytes("empty"), "")
        local big = string.rep("z", 200)
        local bi = must(obs:put({ name = "dir/big.bin", description = "big" }, big, { chunk_size = 80 }))
        eq(bi.chunks, 3)
        eq(obs:get("dir/big.bin"), big)
        local gi = must(obs:get_info("dir/big.bin"))
        eq(gi.description, "big")
        assert(obs:put("ow", "first"))
        eq(obs:put("ow", "second").name or "ow", "ow")
        eq(obs:get("ow"), "second")
        local src, dst = os.tmpname(), os.tmpname()
        local f = assert(io.open(src, "wb"))
        f:write("file-bytes")
        f:close()
        local fi = must(obs:put_file(src, "file.bin"))
        eq(fi.size, 10)
        assert(obs:get_file("file.bin", dst))
        local rf = assert(io.open(dst, "rb"))
        eq(rf:read("*a"), "file-bytes")
        rf:close()
        os.remove(src)
        os.remove(dst)
        assert(obs:put("tmp", "data"))
        assert(obs:update_meta("tmp", { name = "renamed", description = "new desc" }))
        eq(obs:get_info("renamed").description, "new desc")
        assert(obs:delete("renamed"))
        local _, del_err = obs:get("renamed")
        eq(del_err, "object not found")
        eq(must(obs:get_info("renamed", true)).deleted, true)
        local list = must(obs:list())
        local found = false
        for _, item in ipairs(list) do if item.name == "hello.txt" then found = true end end
        assert(found, "list")
        local target = must(obs:get_info("hello.txt"))
        assert(obs2:add_link("link-to-hello", target))
        eq(obs2:get("link-to-hello"), "hello world")
        assert(obs2:add_bucket_link("bucket-link", obs))
        eq(must(obs2:get_info("bucket-link")).options.link.bucket, b)
        local st = must(obs:status())
        eq(st.bucket, b)
        eq(st.sealed, false)
        eq(st.backing_store, "JetStream")
        local sealed = must(obs2:seal())
        eq(sealed.sealed, true)
        eq(obs2:put("after-seal", "x"), nil)
        assert(obs:destroy())
        for i = #owned, 1, -1 do if owned[i] == "OBJ_" .. b then table.remove(owned, i) end end
        assert(nc:delete_object_store(b2))
        for i = #owned, 1, -1 do if owned[i] == "OBJ_" .. b2 then table.remove(owned, i) end end
    end)

    test("object watch and watcher", function()
        local b = "fn" .. RID .. "objw"
        local obs = must(nc:create_object_store({ bucket = b, storage = "memory" }))
        own("OBJ_" .. b)
        assert(obs:put("a", "1"))
        local names, marker = {}, false
        assert(obs:watch(function(info)
            if info == nil then marker = true return false end
            names[#names + 1] = info.name
        end, { timeout = 5 }))
        eq(marker, true)
        assert(has(names, "a"))
        local wn, mark2 = {}, false
        local w = must(obs:watcher(function(info)
            if info then wn[#wn + 1] = info.name else mark2 = true end
        end))
        assert(w:wait_initial(5))
        eq(mark2, true)
        obs:put("via-watcher", "1")
        assert(pump(4, function() return wn[#wn] == "via-watcher" end), "watcher update")
        w:stop()
    end)

    test("micro service", function()
        local bad, berr = nats.add_service(nc, { name = "bad name", version = "1.2.3" })
        eq(bad, nil)
        assert(berr:find("name", 1, true))
        bad, berr = nc:add_service({ name = "okname", version = "01.0.0" })
        eq(bad, nil)
        assert(berr:find("version", 1, true))
        local svc = own_svc(must(nc:add_service({
            name = "fn" .. RID .. "svc", version = "1.0.0-rc.1+b5", description = "test",
            metadata = { k = "v" }, stats_handler = function() return { custom = true } end,
        })))
        eq(svc:is_stopped(), false)
        eq(svc:ping_response().name, svc.name)
        local g = must(svc:add_group(S .. ".svc"))
        local g2 = must(g:add_group("sub"))
        local seen_h
        assert(g2:add_endpoint("echo", function(req)
            seen_h = req:headers()["X-A"]
            req:respond("re:" .. req.data, { ["Y"] = "2" })
        end))
        assert(g:add_endpoint("sum", function(req)
            local d = nats.json.decode(req.data)
            req:respond_json({ sum = d.a + d.b })
        end))
        assert(g:add_endpoint("bad", function(req) req:respond_error(422, "nope") end))
        assert(g:add_endpoint("boom", function() error("kaput") end))
        assert(svc:add_endpoint("noq", function(req) req:respond("x") end, {
            subject = S .. ".svc.noq", queue_group = false,
        }))
        nc:flush()
        local echo = must(nc:get(S .. ".svc.sub.echo", "hi", { ["X-A"] = "1" }))
        eq(echo.data, "re:hi")
        eq(seen_h, "1")
        eq(echo:headers()["Y"], "2")
        local sum = nats.json.decode(must(nc:get(S .. ".svc.sum", '{"a":2,"b":3}')).data)
        eq(sum.sum, 5)
        local er = nc:get(S .. ".svc.bad", "")
        local code, desc = nats.micro.response_error(er)
        eq(code, 422)
        eq(desc, "nope")
        eq(nats.micro.response_error(nc:get(S .. ".svc.boom", "")), 500)
        local pings = must(nc:services_ping(svc.name))
        eq(#pings, 1)
        eq(pings[1].id, svc.id)
        eq(#must(nats.micro.ping(nc, svc.name)), 1)
        local info = must(nc:services_info(svc.name, svc.id))[1]
        eq(info.metadata.k, "v")
        eq(#info.endpoints, 5)
        local noq
        for _, ep in ipairs(info.endpoints) do if ep.name == "noq" then noq = ep end end
        eq(noq.queue_group, "")
        local st = must(nats.micro.stats(nc, svc.name))[1]
        local tot, errs = 0, 0
        for _, ep in ipairs(st.endpoints) do
            tot = tot + ep.num_requests
            errs = errs + ep.num_errors
            if ep.name == "echo" then eq(ep.data.custom, true) end
        end
        eq(tot, 4)
        eq(errs, 2)
        eq(#must(nc:services_stats(svc.name)), 1)
        svc:reset()
        eq(svc:stats().endpoints[1].num_requests, 0)
        svc:stop()
        eq(svc:is_stopped(), true)
        eq(#(nats.micro.ping(nc, svc.name) or {}), 0)
        for i = #svcs, 1, -1 do svcs[i] = nil end
    end)

    test("no leftover streams", function()
        local names = nc:stream_names() or {}
        for _, n in ipairs(names) do
            if n:find(RID, 1, true) then pcall(nc.delete_stream, nc, n) end
        end
        names = must(nc:stream_names())
        local left = {}
        for _, n in ipairs(names) do
            if n:find(RID, 1, true) then left[#left + 1] = n end
        end
        if #left > 0 then error("leftover " .. table.concat(left, ",")) end
    end)
end

io.stdout:setvbuf("line")
local ok_run, run_err = xpcall(run_tests, debug.traceback)
pcall(function() if nc then nc:close() end end)
if not ok_run then
    failed = failed + 1
    print(run_err)
end
stop_servers()
print(string.format("%d passed, %d failed", passed, failed))
os.exit(failed == 0 and 0 or 1)
