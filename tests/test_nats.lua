-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Tests for nats.lua against a NATS server on 127.0.0.1:4222.
-- A short-lived nats-server on 127.0.0.1 covers auth and lame duck.
-- usage: lua tests/test_nats.lua   (run from the repository root)
package.path = "./?.lua;./tests/?.lua;" .. package.path

local nats = require("nats")
local json = nats.json
local socket = require("socket")
local nuid = require("nuid")

local URL = "nats://127.0.0.1:4222"
local P = "nats-lua.unit." .. nuid.next():sub(1, 10)
local passed, failed = 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then passed = passed + 1 else failed = failed + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, msg)
    if a ~= b then error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end
end

local function dial(opts)
    opts = opts or {}
    if not opts.servers and not opts.host then opts.servers = { URL } end
    if opts.timeout == nil then opts.timeout = 2 end
    local c = nats.client(opts)
    local ok, err = pcall(function() assert(c:connect()) end)
    if not ok then error("connect " .. tostring(opts.servers and opts.servers[1] or URL) .. ": " .. tostring(err), 0) end
    return c
end

local function wait_port(port)
    for _ = 1, 50 do
        local s = socket.tcp(); s:settimeout(0.1)
        if s:connect("127.0.0.1", port) then s:close() return true end
        s:close(); socket.sleep(0.05)
    end
    return false
end

local function http_get(port, path)
    local s = socket.tcp(); s:settimeout(2)
    local ok = s:connect("127.0.0.1", port)
    if not ok then s:close() return nil, "monitor down" end
    s:send("GET " .. path .. " HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    local parts = {}
    while true do
        local chunk, _, partial = s:receive(4096)
        if chunk then parts[#parts + 1] = chunk
        else
            if partial and partial ~= "" then parts[#parts + 1] = partial end
            break
        end
    end
    s:close()
    local body = table.concat(parts)
    local i = body:find("\r\n\r\n", 1, true)
    return i and body:sub(i + 4) or body
end

-- LuaSocket TCP objects are userdata, so the wrappers below replace client.socket
-- with a table that forwards to the live socket.
local function proxy_socket(sock, hooks)
    hooks = hooks or {}
    local proxy = {}
    function proxy:receive(pat, prefix)
        if hooks.receive then return hooks.receive(sock, pat, prefix) end
        return sock:receive(pat, prefix)
    end
    function proxy:send(data, i, j)
        if hooks.send then return hooks.send(sock, data, i, j) end
        return sock:send(data, i, j)
    end
    function proxy:settimeout(t) return sock:settimeout(t) end
    function proxy:close() return sock:close() end
    function proxy:shutdown() if sock.shutdown then return sock:shutdown() end end
    function proxy:dirty() return sock.dirty and sock:dirty() end
    function proxy:getfd() return sock.getfd and sock:getfd() end
    function proxy:setoption(opt, val) if sock.setoption then return sock:setoption(opt, val) end end
    return proxy
end

-- One new byte per receive, so the partial-read path runs on frames from the real server.
local function drip(client)
    client.socket = proxy_socket(client.socket, { receive = function(raw, pat, prefix)
        prefix = prefix or ""
        if pat == "*l" then
            local buf = prefix
            if buf:sub(-1) == "\n" then
                if buf:sub(-2, -2) == "\r" then return buf:sub(1, -3) end
                return buf:sub(1, -2)
            end
            local b, err = raw:receive(1)
            if not b then return nil, err, buf ~= "" and buf or nil end
            buf = buf .. b
            if b == "\n" then
                if buf:sub(-2, -2) == "\r" then return buf:sub(1, -3) end
                return buf:sub(1, -2)
            end
            return nil, "timeout", buf
        elseif type(pat) == "number" then
            if #prefix >= pat then return prefix:sub(1, pat) end
            local b, err = raw:receive(1)
            if not b then return nil, err, prefix ~= "" and prefix or nil end
            local buf = prefix .. b
            if #buf >= pat then return buf end
            return nil, "timeout", buf
        end
        return raw:receive(pat, prefix ~= "" and prefix or nil)
    end })
end

-- A conforming server answers PING. Dropping our own PING leaves pongs unmatched.
local function drop_pings(client)
    client.socket = proxy_socket(client.socket, { send = function(raw, data, i, j)
        if data == "PING\r\n" then return #data end
        return raw:send(data, i, j)
    end })
end

local nsub = 0
local function subj()
    nsub = nsub + 1
    return P .. "." .. nsub
end

local function bye(...)
    for i = 1, select("#", ...) do
        local c = select(i, ...)
        if c then pcall(function() c:close() end) end
    end
end

-- Auth, nkey, and lame duck need a server that enforces them. The broker on
-- 4222 accepts any credentials and must not be put into lame duck.
local auth_port, auth_mon, auth_user, auth_stop, auth_pid = 14321, 18321
local function ensure_auth_server()
    if auth_stop then return end
    local nkeys = require("nats-nkeys")
    auth_user = nkeys.create_pair("user")
    local dir = os.tmpname(); os.remove(dir); assert(os.execute("mkdir -p " .. dir))
    local conf = string.format([[
listen: 127.0.0.1:%d
http: 127.0.0.1:%d
lame_duck_duration: "30s"
lame_duck_grace_period: "10s"
max_payload: 32
authorization {
  users = [
    { user: "alice", password: "secret" }
    { nkey: %q }
  ]
}
]], auth_port, auth_mon, auth_user:public_key())
    local f = assert(io.open(dir .. "/nats.conf", "w")); f:write(conf); f:close()
    local bin = os.getenv("NATS_SERVER_BIN") or "nats-server"
    auth_pid = dir .. "/nats.pid"
    assert(os.execute(bin .. " -c " .. dir .. "/nats.conf -P " .. auth_pid .. " > " .. dir .. "/nats.log 2>&1 &"))
    if not wait_port(auth_port) then
        local log = io.open(dir .. "/nats.log")
        error("auth server did not start: " .. (log and log:read("*a") or "no log"))
    end
    auth_stop = function()
        local pf = io.open(auth_pid)
        if pf then
            local pid = pf:read("*l")
            pf:close()
            if pid and pid ~= "" then os.execute("kill -9 " .. pid .. " 2>/dev/null") end
        end
        for _ = 1, 50 do
            local a, b = socket.tcp(), socket.tcp()
            a:settimeout(0.05); b:settimeout(0.05)
            local up = a:connect("127.0.0.1", auth_port) or b:connect("127.0.0.1", auth_mon)
            a:close(); b:close()
            if not up then break end
            socket.sleep(0.05)
        end
        os.execute("rm -rf " .. dir)
        auth_stop = nil
    end
end

local function signal_ldm()
    ensure_auth_server()
    local bin = os.getenv("NATS_SERVER_BIN") or "nats-server"
    assert(os.execute(bin .. " --signal ldm=" .. auth_pid .. " >/dev/null 2>&1"))
end

-- helpers ------------------------------------------------------------------
test("json codec (active backend) round trip", function()
    local t = json.decode(json.encode({ a = 1, b = { 1, 2, 3 }, c = "x\"y\n", d = true }))
    eq(t.a, 1); eq(t.b[3], 3); eq(t.c, "x\"y\n"); eq(t.d, true)
end)

test("integers: nats.int and json decode on Lua 5.3+", function()
    eq(nats.int("7"), 7); eq(nats.int(2.5), 2.5); eq(nats.int("x"), nil)
    local t = nats.json.decode('{"seq":12,"f":1.5,"n":{"a":[3]}}')
    eq(t.seq, 12); eq(t.f, 1.5); eq(t.n.a[1], 3)
    if math.type then
        eq(math.type(nats.int(3.0)), "integer")
        eq(math.type(t.seq), "integer"); eq(math.type(t.n.a[1]), "integer"); eq(math.type(t.f), "float")
    end
end)

test("pure Lua json fallback", function()
    local saved = package.loaded.nats
    package.loaded.nats = nil
    package.preload.cjson = function() error("no cjson") end
    local saved_cjson = package.loaded.cjson
    package.loaded.cjson = nil
    local n2 = require("nats")
    package.preload.cjson = nil
    package.loaded.cjson = saved_cjson
    package.loaded.nats = saved
    eq(n2.json.backend, "lua")
    local j = n2.json
    local s = j.encode({ name = "a\tb", list = { 1, 2.5, "z" }, empty = {}, flag = false, big = 120000000000 })
    local t = j.decode(s)
    eq(t.name, "a\tb"); eq(t.list[2], 2.5); eq(t.flag, false); eq(t.big, 120000000000)
    assert(s:find('"big":120000000000', 1, true), s)
    local u = j.decode('{"k":"\\u00e9\\ud83d\\ude00","n":null,"neg":-1.5e2,"arr":[]}')
    eq(u.k, "\195\169\240\159\152\128"); eq(u.n, nil); eq(u.neg, -150); eq(#u.arr, 0)
    assert(not pcall(j.decode, '{"a":1'))
end)

test("header encode/decode (ADR-4)", function()
    local h = nats.encode_header({ A = "1", B = { "x", "y" } })
    assert(h:find("A: 1\r\n", 1, true))
    assert(h:find("B: x\r\nB: y\r\n", 1, true), "multi values one per line")
    local d = nats.decode_header("A: 1\r\nB: x\r\nB: y\r\nC: z\r\nC: w\r\nC: v\r\n")
    eq(d.A, "1"); eq(d.B[1], "x"); eq(d.B[2], "y"); eq(#d.C, 3)
    local s = nats.decode_header("NATS/1.0 503 No Responders\r\n\r\n")
    eq(s.Status, "503"); eq(s.Description, "No Responders")
    eq(nats.decode_header("Key:  spaced value  \r\n").Key, "spaced value")
end)

test("subject validation (ADR-6)", function()
    assert(nats.valid_subject("foo.bar"))
    assert(nats.valid_subject("foo.*.baz"))
    assert(nats.valid_subject("foo.>"))
    assert(nats.valid_subject(">"))
    assert(not nats.valid_subject(""))
    assert(not nats.valid_subject("foo..bar"))
    assert(not nats.valid_subject(".foo"))
    assert(not nats.valid_subject("foo."))
    assert(not nats.valid_subject("foo bar"))
    assert(not nats.valid_subject("foo\tbar"))
    assert(not nats.valid_subject("foo.>.bar"))
    assert(not nats.valid_subject("foo.b*"))
    assert(not nats.valid_publish_subject("foo.*"))
    assert(nats.valid_publish_subject("$JS.API.INFO"))
    assert(nats.valid_queue("q1") and not nats.valid_queue("q 1"))
end)

test("parse_url", function()
    local s = nats.parse_url("nats://u:p@example.com:4333")
    eq(s.host, "example.com"); eq(s.port, 4333); eq(s.user, "u"); eq(s.pass, "p")
    s = nats.parse_url("tls://tok@h")
    eq(s.token, "tok"); eq(s.port, 4222); eq(s.tls, true)
    s = nats.parse_url("10.0.0.1:4223"); eq(s.host, "10.0.0.1"); eq(s.port, 4223)
    s = nats.parse_url("nats://[::1]:4224"); eq(s.host, "::1"); eq(s.port, 4224)
end)

test("PUB/HPUB wire format (ADR-4 examples)", function()
    local c = { server_information = { max_payload = 100 }, error = nats.error }
    eq(nats.msg_string(c, "SUBJECT", "PAYLOAD", "REPLY", "Header: X\r\n"),
        "HPUB SUBJECT REPLY 23 30\r\nNATS/1.0\r\nHeader: X\r\n\r\nPAYLOAD\r\n")
    eq(nats.msg_string(c, "SUBJECT", "", "REPLY", "Header: X"),
        "HPUB SUBJECT REPLY 23 23\r\nNATS/1.0\r\nHeader: X\r\n\r\n\r\n")
    eq(nats.msg_string(c, "S", "hi"), "PUB S 2\r\nhi\r\n")
    eq(nats.msg_string(c, "S", nil, "R"), "PUB S R 0\r\n\r\n")
    assert(not pcall(nats.msg_string, c, "S", string.rep("x", 101)), "max_payload")
end)

-- connection ---------------------------------------------------------------
test("new_inbox honours inbox_prefix; create_inbox stays on the mux", function()
    local c = nats.client({ inbox_prefix = "lab.ibx" })
    local a = c:new_inbox()
    assert(a:sub(1, 8) == "lab.ibx.", a)
    assert(not a:sub(9):find(".", 1, true), "new_inbox is prefix plus one token")
    assert(#a:sub(9) == 22, "mailbox token is a 22-char nuid")
    assert(a ~= c:new_inbox())
    local ibx, lid = c:create_inbox()
    assert(ibx == c.inbox_muxed .. "." .. lid, ibx)
    assert(c.inbox_muxed:sub(1, 8) == "lab.ibx.")
    assert(#lid == 8)
    local mod = nats.new_inbox("lab.ibx")
    assert(mod:sub(1, 8) == "lab.ibx." and not mod:sub(9):find(".", 1, true))
    assert(select(2, nats.new_inbox()) == nil)
end)

test("connect handshake against 127.0.0.1:4222", function()
    local name = P .. ".conn"
    local c = dial({ name = name, ignore_discovered_servers = true })
    eq(c.connected, true)
    eq(c.current_server.host, "127.0.0.1")
    eq(c.current_server.port, 4222)
    local si = c.server_information
    assert(type(si.server_id) == "string" and #si.server_id > 0)
    eq(si.headers, true)
    assert((si.proto or 0) >= 1)
    eq(c.lame_duck, false)
    assert(c:flush(2))
    local got
    local s = subj()
    local sid = c:subscribe(s, function(m) got = m.data end)
    c:flush(1)
    c:publish(s, "ping")
    c:wait({ sid = sid, timeout = 1 })
    eq(got, "ping")
    local body = http_get(8222, "/connz?limit=1024")
    assert(body and body:find(name, 1, true), "connz did not list " .. name .. ": " .. tostring(body and body:sub(1, 180)))
    assert(body:find('"lang": "lua"', 1, true) or body:find('"lang":"lua"', 1, true), "connz lang")
    bye(c)
end)

test("connect: legacy host/port arguments and nats.connect returns client", function()
    local c = nats.connect("127.0.0.1", 4222)
    eq(c.connected, true)
    eq(c.current_server.host, "127.0.0.1")
    eq(c.current_server.port, 4222)
    bye(c)
end)

test("connect: tries the next server in the pool", function()
    local c = nats.client({
        servers = "nats://127.0.0.1:1,nats://127.0.0.1:4222",
        no_randomize = true, ignore_discovered_servers = true, timeout = 2,
    })
    assert(c:connect())
    eq(c.current_server.host, "127.0.0.1")
    eq(c.current_server.port, 4222)
    bye(c)
end)

test("MSG with CRLF and binary inside payload, split reads", function()
    local c = dial({ ignore_discovered_servers = true })
    local got = {}
    local s = subj()
    local sid = c:subscribe(s, function(m) got[#got + 1] = m end)
    c:flush(1)
    drip(c)
    local p1 = "line1\r\nline2\n\0\255end"
    c:publish(s, p1, "r.1")
    c:publish(s, "xyz")
    local m = c:wait({ maxcount = 2, timeout = 2 })
    eq(#got, 2)
    eq(got[1].data, p1); eq(got[1].reply, "r.1"); eq(got[1].sid, sid)
    eq(got[2].data, "xyz"); eq(got[2].reply, nil)
    eq(m.data, "xyz")
    bye(c)
end)

test("HMSG headers and status, header passthrough", function()
    local c = dial({ ignore_discovered_servers = true })
    local got
    local s = subj()
    local sid = c:subscribe(s, function(m) got = m end)
    c:flush(1)
    c:publish(s, "PAYLOAD", "REPLY", { Header1 = { "X", "Y" }, Header2 = "Z" })
    c:wait({ sid = sid, timeout = 1 })
    eq(got.data, "PAYLOAD"); eq(got.reply, "REPLY")
    local h = got:headers()
    eq(h.Header1[1], "X"); eq(h.Header1[2], "Y"); eq(h.Header2, "Z")
    eq(got:status(), nil)
    bye(c)
end)

test("request/get with muxed inbox", function()
    local c = dial({ ignore_discovered_servers = true })
    local s = subj()
    c:subscribe(s, function(m) m:respond("echo:" .. m.data) end)
    c:flush(1)
    local r = c:get(s, "hi")
    eq(r.data, "echo:hi")
    assert(r.subject:sub(1, #c.inbox_muxed + 1) == c.inbox_muxed .. ".")
    eq(next(c.inbox_table), nil, "inbox entry cleaned up")
    local cb
    local m = c:request(s, function(x) cb = x.data end, "yo")
    c:wait({ lid = m.lid, timeout = 1 })
    eq(cb, "echo:yo")
    bye(c)
end)

test("empty request times out, or reports 503 when the server sends it", function()
    local c = dial({ ignore_discovered_servers = true, timeout = 0.4 })
    local r, err, raw = c:get(subj(), "x")
    eq(r, nil)
    if err == "no responders" then
        eq(raw.status_code, "503")
        eq(raw:status(), 503)
    else
        eq(err, "timeout")
    end
    eq(next(c.inbox_table), nil)
    bye(c)
end)

test("flush answers server PING and ignores +OK", function()
    local errs = {}
    local c = dial({
        verbose = true, ignore_discovered_servers = true,
        error_cb = function(_, e) errs[#errs + 1] = e end,
    })
    c:publish(subj(), "x")
    assert(c:flush(2))
    eq(#errs, 0)
    bye(c)
end)

test("server -ERR maximum payload is reported", function()
    ensure_auth_server()
    local errs = {}
    local c = dial({
        servers = { "nats://alice:secret@127.0.0.1:" .. auth_port },
        ignore_discovered_servers = true,
        error_cb = function(_, e) errs[#errs + 1] = e end,
    })
    c.server_information.max_payload = 64
    local ok, err = pcall(function()
        c:publish(subj(), string.rep("x", 64))
        c:wait({ timeout = 1 })
    end)
    local joined = (table.concat(errs, " ") .. " " .. tostring(err)):lower()
    assert(joined:find("payload", 1, true), "expected payload -ERR, got [" .. joined .. "] ok=" .. tostring(ok))
    bye(c)
end)

test("max_payload from INFO enforced locally", function()
    local c = dial({ ignore_discovered_servers = true })
    c.server_information.max_payload = 10
    c:publish(subj(), "0123456789")
    local ok, err = pcall(c.publish, c, subj(), "01234567890")
    assert(not ok and tostring(err):find("maximum payload"), tostring(err))
    bye(c)
end)

test("invalid subjects rejected before hitting the wire", function()
    local c = dial({ ignore_discovered_servers = true })
    assert(not pcall(c.publish, c, "bad subject", "x"))
    assert(not pcall(c.publish, c, "a..b", "x"))
    assert(not pcall(c.subscribe, c, "a.>.b"))
    assert(not pcall(c.subscribe, c, "a", nil, "bad queue"))
    bye(c)
end)

test("unsubscribe with max auto-removes", function()
    local c = dial({ ignore_discovered_servers = true })
    local n = 0
    local s = subj()
    local sid = c:subscribe(s, function() n = n + 1 end)
    c:unsubscribe(sid, 2)
    c:flush(1)
    c:publish(s, "x"); c:publish(s, "x"); c:publish(s, "x")
    c:wait({ maxcount = 2, timeout = 1 })
    eq(n, 2); eq(c.subscriptions[sid], nil)
    bye(c)
end)

test("wait maxcount processes exactly maxcount messages", function()
    local c = dial({ ignore_discovered_servers = true })
    local n = 0
    local s = subj()
    local sid = c:subscribe(s, function() n = n + 1 end)
    c:flush(1)
    c:publish(s, "x"); c:publish(s, "x"); c:publish(s, "x")
    c:wait({ maxcount = 2, timeout = 1 })
    eq(n, 2)
    bye(c)
end)

test("INFO connect_urls are merged and the connection is not lame duck", function()
    local c = dial()
    assert(#c.servers > 1, "expected discovered connect_urls, have " .. #c.servers)
    eq(c.current_server.host, "127.0.0.1")
    eq(c.current_server.port, 4222)
    eq(c.lame_duck, false)
    bye(c)
end)

test("keepalive: missed PONGs reconnect and resubscribe", function()
    local events = {}
    local c = dial({
        reconnect = true, ping_interval = 0.05, max_pings_out = 2,
        reconnect_wait = 0.01, reconnect_jitter = 0, max_reconnect = -1,
        no_randomize = true, ignore_discovered_servers = true,
        disconnected_cb = function() events[#events + 1] = "down" end,
        reconnected_cb = function() events[#events + 1] = "up" end,
    })
    local got
    local s = subj()
    local sid = c:subscribe(s, function(m) got = m.data end, "q1")
    c:flush(1)
    drop_pings(c)
    local deadline = socket.gettime() + 2
    while c.stats.reconnects < 1 and socket.gettime() < deadline do
        pcall(function() c:wait({ timeout = 0.1 }) end)
    end
    assert(c.stats.reconnects >= 1, "reconnects " .. tostring(c.stats.reconnects))
    eq(c:status_string(), "connected")
    eq(events[1], "down"); eq(events[2], "up")
    local pub = dial({ ignore_discovered_servers = true })
    pub:publish(s, "alive"); pub:flush(1)
    c:wait({ sid = sid, timeout = 1 })
    eq(got, "alive")
    bye(c, pub)
end)

test("socket closed -> reconnect; without reconnect -> error", function()
    local c = dial({
        reconnect = true, reconnect_wait = 0.01, reconnect_jitter = 0,
        no_randomize = true, ignore_discovered_servers = true,
    })
    c.socket:close()
    c:wait({ timeout = 1 })
    eq(c.connected, true)
    assert(c.stats.reconnects >= 1)
    bye(c)
    local c2 = dial({ ignore_discovered_servers = true })
    c2.socket:close()
    local ok, err = pcall(c2.wait, c2, { timeout = 0.5 })
    assert(not ok, tostring(err))
    bye(c2)
end)

test("reconnect gives up after max_reconnect", function()
    local closed = false
    local c = dial({
        reconnect = true, max_reconnect = 2, reconnect_wait = 0.01, reconnect_jitter = 0,
        no_randomize = true, ignore_discovered_servers = true,
        closed_cb = function() closed = true end,
    })
    c:set_server_pool("nats://127.0.0.1:1")
    c.current_server = c.servers[1]
    c.socket:close()
    local ok, err = pcall(function() c:wait({ timeout = 2 }) end)
    assert(not ok and tostring(err):find("reconnect failed"), tostring(err))
    eq(closed, true)
    bye(c)
end)

test("flush and drain", function()
    local c = dial({ ignore_discovered_servers = true })
    assert(c:flush(1))
    local n = 0
    local s = subj()
    c:subscribe(s, function() n = n + 1 end)
    c:flush(1)
    c:publish(s, "x")
    eq(c:drain(2), true)
    eq(n, 1, "in-flight message delivered during drain")
    eq(c.connected, false); eq(c.closed, true)
end)

test("encoded client JSON round trip", function()
    local c = dial({ ignore_discovered_servers = true })
    local s = subj()
    c:subscribe(s, function(m)
        local t = nats.json.decode(m.data)
        m:respond(nats.json.encode({ sum = t.a + t.b }))
    end)
    c:flush(1)
    local ec = c:encoded_client()
    local r = ec:get(s, { a = 2, b = 3 })
    eq(r.data.sum, 5)
    bye(c)
end)

test("creds parsing", function()
    local jwt, seed = nats.read_creds([[
-----BEGIN NATS USER JWT-----
eyJhbGciOi.payload.sig
------END NATS USER JWT------

-----BEGIN USER NKEY SEED-----
SUAEXAMPLESEED
------END USER NKEY SEED------
]])
    eq(jwt, "eyJhbGciOi.payload.sig"); eq(seed, "SUAEXAMPLESEED")
end)

-- The broker on 4222 stays up. Refusing the reconnect dial leaves a window
-- where publishes buffer; the next attempt connects to 127.0.0.1:4222.
test("publish buffering while reconnecting (ADR-40)", function()
    local s = subj()
    local got = {}
    local sub = dial({ ignore_discovered_servers = true })
    sub:subscribe(s, function(m) got[#got + 1] = m.data end)
    sub:flush(1)
    local ev = {}
    local c = dial({
        reconnect = true, reconnect_wait = 0.4, reconnect_jitter = 0, max_reconnect = -1,
        no_randomize = true, ignore_discovered_servers = true,
        disconnected_cb = function() ev[#ev + 1] = "down" end,
        reconnected_cb = function() ev[#ev + 1] = "up" end,
    })
    local orig = nats.socket_factory
    local held = true
    nats.socket_factory = function(srv, cfg)
        if held then return nil, "down" end
        return orig(srv, cfg)
    end
    local ok, err = pcall(function()
        c.socket:close()
        c:wait({ timeout = 0.15 })
        eq(c:status_string(), "reconnecting")
        local t0 = socket.gettime()
        c:publish(s, "one")
        c:publish(s, "two")
        assert(socket.gettime() - t0 < 0.2, "publish did not return while reconnecting")
        assert(c:buffered() > 0, "buffered while down")
        held = false
        local deadline = socket.gettime() + 3
        while #got < 2 and socket.gettime() < deadline do
            c:wait({ timeout = 0.05 })
            sub:wait({ timeout = 0.05 })
        end
        eq(c:status_string(), "connected")
        eq(c:buffered(), 0)
        eq(#got, 2); eq(got[1], "one"); eq(got[2], "two")
        eq(ev[1], "down"); eq(ev[2], "up")
    end)
    nats.socket_factory = orig
    bye(c, sub)
    if not ok then error(err, 0) end
end)

test("reconnect buffer limit", function()
    local c = dial({
        reconnect = true, reconnect_wait = 2, reconnect_jitter = 0, max_reconnect = -1,
        reconnect_buf_size = 10, no_randomize = true, ignore_discovered_servers = true,
    })
    c:set_server_pool("nats://127.0.0.1:1")
    c.current_server = c.servers[1]
    c.socket:close()
    c:wait({ timeout = 0.2 })
    eq(c:status_string(), "reconnecting")
    local ok, err = pcall(c.publish, c, subj(), string.rep("y", 20))
    assert(not ok and tostring(err):find("reconnect buffer"), tostring(err))
    bye(c)
end)

test("tickers fire from the pump loop", function()
    local c = dial({ ignore_discovered_servers = true })
    local n = 0
    c:add_ticker(0.01, function() n = n + 1; if n == 3 then return false end end)
    c:wait({ timeout = 0.2 })
    eq(n, 3)
    bye(c)
end)

test("connect: wrong password is rejected, the right password connects", function()
    ensure_auth_server()
    local bad = nats.client({
        servers = { "nats://alice:wrong@127.0.0.1:" .. auth_port },
        timeout = 2, ignore_discovered_servers = true,
    })
    local ok, err = pcall(bad.connect, bad)
    assert(not ok and tostring(err):lower():find("authorization"), tostring(err))
    bye(bad)
    local c = dial({
        servers = { "nats://alice:secret@127.0.0.1:" .. auth_port },
        ignore_discovered_servers = true,
    })
    assert(c:flush(1))
    local got
    local s = subj()
    local sid = c:subscribe(s, function(m) got = m.data end)
    c:flush(1)
    c:publish(s, "auth")
    c:wait({ sid = sid, timeout = 1 })
    eq(got, "auth")
    bye(c)
end)

test("nkey seed auth against 127.0.0.1", function()
    ensure_auth_server()
    local c = dial({
        servers = { "nats://127.0.0.1:" .. auth_port },
        nkey_seed = auth_user:seed(), ignore_discovered_servers = true,
    })
    assert(c:flush(1))
    local got
    local s = subj()
    local sid = c:subscribe(s, function(m) got = m.data end)
    c:publish(s, "ok"); c:wait({ sid = sid, timeout = 1 })
    eq(got, "ok")
    bye(c)
    local nkeys = require("nats-nkeys")
    local bad = nats.client({
        servers = { "nats://127.0.0.1:" .. auth_port },
        nkey_seed = nkeys.create_pair("user"):seed(),
        timeout = 2, ignore_discovered_servers = true,
    })
    local ok, err = pcall(bad.connect, bad)
    assert(not ok and tostring(err):lower():find("authorization"), tostring(err))
    bye(bad)
end)

test("nkey auth uses signature_cb with the server nonce", function()
    ensure_auth_server()
    local seen
    local c = nats.client({
        servers = { "nats://127.0.0.1:" .. auth_port },
        nkey = auth_user:public_key(),
        signature_cb = function(nonce)
            seen = nonce
            return auth_user:sign_nonce(nonce)
        end,
        timeout = 2, ignore_discovered_servers = true,
    })
    assert(c:connect())
    assert(c:flush(1))
    assert(type(seen) == "string" and #seen > 0, "nonce")
    bye(c)
    local bad = nats.client({
        servers = { "nats://127.0.0.1:" .. auth_port },
        nkey = auth_user:public_key(),
        signature_cb = function() return "nope" end,
        timeout = 2, ignore_discovered_servers = true,
    })
    local ok, err = pcall(bad.connect, bad)
    assert(not ok, tostring(err))
    bye(bad)
end)

test("lame duck callback from the private server", function()
    ensure_auth_server()
    local ldm = false
    local c = dial({
        servers = { "nats://alice:secret@127.0.0.1:" .. auth_port },
        ignore_discovered_servers = true,
        lame_duck_cb = function() ldm = true end,
    })
    signal_ldm()
    local deadline = socket.gettime() + 3
    while not ldm and socket.gettime() < deadline do
        pcall(function() c:wait({ timeout = 0.2 }) end)
    end
    eq(ldm, true)
    eq(c.lame_duck, true)
    bye(c)
end)

test("request_many: max_messages, stall and sentinel (ADR-47)", function()
    local c = dial({ ignore_discovered_servers = true })
    local s = subj()
    c:subscribe(s, function(m)
        for i = 1, 5 do c:publish(m.reply, tostring(i)) end
        c:publish(m.reply, "")
    end)
    c:flush(1)
    eq(#c:request_many(s, "", { max_messages = 3 }), 3)
    eq(#c:request_many(s, "", { sentinel = true, max_wait = 1 }), 5)
    local t0 = socket.gettime()
    eq(#c:request_many(s, "", { stall = 0.05, max_wait = 2 }), 6)
    assert(socket.gettime() - t0 < 1, "stall ended the request")
    bye(c)
end)

test("websocket framing round trip", function()
    local f = nats.ws_frame(2, "PING\r\n", false)
    eq(#f, 2 + 4 + 6)
    eq(f:byte(1), 0x82); eq(f:byte(2), 0x80 + 6)
    local big = nats.ws_frame(2, string.rep("x", 70000), true)
    eq(big:byte(2), 0x80 + 127)
end)

test("url parsing: ws/wss and paths", function()
    local s = nats.parse_url("wss://user:pw@example.com/nats")
    eq(s.ws, true); eq(s.tls, true); eq(s.port, 443); eq(s.ws_path, "/nats"); eq(s.user, "user")
    local w = nats.parse_url("ws://127.0.0.1:8080")
    eq(w.port, 8080); eq(w.ws_path, "/")
end)

test("contexts (ADR-21)", function()
    local dir = os.tmpname(); os.remove(dir)
    os.execute("mkdir -p " .. dir .. "/nats/context")
    local f = io.open(dir .. "/nats/context/t.json", "w")
    f:write('{"url":"nats://127.0.0.1:4222,nats://127.0.0.1:4223","user":"u","password":"p","token":"","jetstream_domain":"hub"}')
    f:close()
    f = io.open(dir .. "/nats/context.txt", "w"); f:write("t\n"); f:close()
    local old = os.getenv("XDG_CONFIG_HOME")
    nats.context_dir = function() return dir .. "/nats" end
    local o = assert(nats.load_context())
    eq(o.servers, "nats://127.0.0.1:4222,nats://127.0.0.1:4223"); eq(o.user, "u"); eq(o.pass, "p"); eq(o.auth_token, nil)
    eq(o.jetstream.domain, "hub")
    os.execute("rm -rf " .. dir)
    _ = old
end)

if auth_stop then auth_stop() end
print(string.format("%s: %d passed, %d failed", jit and jit.version or _VERSION, passed, failed))
os.exit(failed == 0 and 0 or 1)
