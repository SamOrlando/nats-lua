-- Unit tests for nats.lua using a fake socket (no network needed).
-- usage: lua tests/test_nats.lua   (run from the repository root)
package.path = "./?.lua;./tests/?.lua;" .. package.path

local fakesock = require("fakesock")
local nats = require("nats")
local json = nats.json

local passed, failed = 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then passed = passed + 1 else failed = failed + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, msg)
    if a ~= b then error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end
end

local INFO = { server_id = "TEST", version = "2.10.23", proto = 1, headers = true, max_payload = 1048576 }

-- scripted fake server: answers PING with PONG, optional request responder
local function fake_server(o)
    o = o or {}
    local st = { socks = {}, connects = {} }
    nats.socket_factory = function(srv, cfg)
        if o.refuse and o.refuse(srv, #st.socks) then return nil, "connection refused" end
        local info = o.info or INFO
        local s = fakesock.new(o.greeting or ("INFO " .. json.encode(info) .. "\r\n"))
        s.srv = srv
        s.on_send = function(sock, data)
            local i = 1
            while i <= #data do
                local e = data:find("\r\n", i, true)
                if not e then break end
                local line = data:sub(i, e - 1)
                i = e + 2
                local verb = line:match("^(%S+)")
                if verb == "PING" then
                    if not st.no_pong then sock:feed("PONG\r\n") end
                elseif verb == "CONNECT" then
                    st.connects[#st.connects + 1] = json.decode(line:sub(9))
                    if o.connect_err then sock:feed("-ERR '" .. o.connect_err .. "'\r\n") end
                elseif verb == "PUB" or verb == "HPUB" then
                    local args = {}
                    for a in line:gmatch("%S+") do args[#args + 1] = a end
                    local size = tonumber(args[#args])
                    local payload = data:sub(i, i + size - 1)
                    i = i + size + 2
                    if o.on_pub then o.on_pub(sock, args, payload) end
                end
            end
        end
        st.socks[#st.socks + 1] = s
        st.last = s
        return s
    end
    return st
end

-- helpers ------------------------------------------------------------------
test("json codec (active backend) round trip", function()
    local t = json.decode(json.encode({ a = 1, b = { 1, 2, 3 }, c = "x\"y\n", d = true }))
    eq(t.a, 1); eq(t.b[3], 3); eq(t.c, "x\"y\n"); eq(t.d, true)
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
test("connect handshake: INFO before CONNECT, CONNECT fields, PING/PONG", function()
    local st = fake_server()
    local c = nats.client({ servers = { "nats://alice:secret@h1:4222" }, name = "t" })
    assert(c:connect())
    eq(c.connected, true)
    local cn = st.connects[1]
    eq(cn.user, "alice"); eq(cn.pass, "secret"); eq(cn.headers, true); eq(cn.no_responders, true)
    eq(cn.protocol, 1); eq(cn.name, "t"); eq(cn.lang, "lua")
    local w = st.last:written()
    assert(w:find("^CONNECT {"), "CONNECT is the first thing written")
    assert(w:find("PING\r\n", 1, true))
    assert(w:find("SUB " .. c.inbox_prefix .. ".* 1\r\n", 1, true), "muxed inbox subscription")
    eq(c.server_information.server_id, "TEST")
end)

test("connect: legacy host/port arguments and nats.connect returns client", function()
    local st = fake_server()
    local c = nats.connect("hostx", 4999)
    eq(st.last.srv.host, "hostx"); eq(st.last.srv.port, 4999)
    eq(c.connected, true)
end)

test("connect: -ERR (auth) raises", function()
    fake_server({ connect_err = "Authorization Violation" })
    local c = nats.client()
    local ok, err = pcall(c.connect, c)
    assert(not ok and tostring(err):find("Authorization Violation"), tostring(err))
end)

test("connect: tries next server in pool", function()
    local st = fake_server({ refuse = function(srv) return srv.host == "bad" end })
    local c = nats.client({ servers = "nats://bad:1,nats://good:2", no_randomize = true })
    assert(c:connect())
    eq(c.current_server.host, "good")
end)

-- parsing ------------------------------------------------------------------
test("MSG with CRLF and binary inside payload, split reads", function()
    local st = fake_server()
    local c = nats.client(); c:connect()
    local got = {}
    local sid = c:subscribe("a.b", function(m) got[#got + 1] = m end)
    local p1 = "line1\r\nline2\n\0\255end"
    local frame = "MSG a.b " .. sid .. " r.1 " .. #p1 .. "\r\n" .. p1 .. "\r\n" ..
        "MSG a.b " .. sid .. " 3\r\nxyz\r\n"
    -- deliver one byte at a time to exercise partial-read carry-over
    local pos = 0
    st.last.on_empty = function()
        pos = pos + 1
        if pos <= #frame then return frame:sub(pos, pos) end
    end
    local m = c:wait({ maxcount = 2, timeout = 2 })
    eq(#got, 2)
    eq(got[1].data, p1); eq(got[1].reply, "r.1"); eq(got[1].sid, sid)
    eq(got[2].data, "xyz"); eq(got[2].reply, nil)
    eq(m.data, "xyz")
end)

test("HMSG headers and status, header passthrough", function()
    local st = fake_server()
    local c = nats.client(); c:connect()
    local got
    local sid = c:subscribe("h", function(m) got = m end)
    local hdr = "NATS/1.0\r\nHeader1: X\r\nHeader1: Y\r\nHeader2: Z\r\n\r\n"
    st.last:feed("HMSG h " .. sid .. " REPLY " .. #hdr .. " " .. (#hdr + 7) .. "\r\n" .. hdr .. "PAYLOAD\r\n")
    c:wait({ sid = sid, timeout = 1 })
    eq(got.data, "PAYLOAD"); eq(got.reply, "REPLY")
    eq(got.header, "Header1: X\r\nHeader1: Y\r\nHeader2: Z\r\n")
    local h = got:headers()
    eq(h.Header1[2], "Y"); eq(h.Header2, "Z")
    eq(got:status(), nil)
end)

test("request/get with muxed inbox", function()
    local st = fake_server({ on_pub = function(sock, args, payload)
        if args[2] == "svc.echo" then
            sock:feed("MSG " .. args[3] .. " 1 " .. (#payload + 5) .. "\r\necho:" .. payload .. "\r\n")
        end
    end })
    local c = nats.client(); c:connect()
    local r = c:get("svc.echo", "hi")
    eq(r.data, "echo:hi")
    assert(r.subject:find(c.inbox_prefix, 1, true) == 1)
    eq(next(c.inbox_table), nil, "inbox entry cleaned up")
    -- async request with callback
    local cb
    local m = c:request("svc.echo", function(x) cb = x.data end, "yo")
    c:wait({ lid = m.lid, timeout = 1 })
    eq(cb, "echo:yo")
end)

test("no responders (503) and timeout", function()
    local st = fake_server({ on_pub = function(sock, args)
        if args[2] == "nobody" then
            local h = "NATS/1.0 503\r\n\r\n"
            sock:feed("HMSG " .. args[3] .. " 1 " .. #h .. " " .. #h .. "\r\n" .. h .. "\r\n")
        end
    end })
    local c = nats.client({ timeout = 0.2 }); c:connect()
    local r, err = c:get("nobody", "x")
    eq(r, nil); eq(err, "no responders")
    local _, _, raw = c:get("nobody", "x")
    eq(raw.status_code, "503"); eq(raw:status(), 503)
    r, err = c:get("silent", "x")
    eq(r, nil); eq(err, "timeout")
    eq(next(c.inbox_table), nil)
end)

test("server PING answered with PONG, +OK ignored, -ERR reported", function()
    local st = fake_server()
    local errs = {}
    local c = nats.client({ error_cb = function(_, e) errs[#errs + 1] = e end }); c:connect()
    st.last:clear()
    st.last:feed("PING\r\n+OK\r\n-ERR 'Permissions Violation for Publish to \"x\"'\r\n")
    c:wait({ timeout = 0.05 })
    assert(st.last:written():find("PONG\r\n", 1, true))
    eq(errs[1], 'Permissions Violation for Publish to "x"')
end)

test("max_payload from INFO enforced locally", function()
    local info = {}; for k, v in pairs(INFO) do info[k] = v end
    info.max_payload = 10
    fake_server({ info = info })
    local c = nats.client(); c:connect()
    c:publish("x", "0123456789")
    local ok, err = pcall(c.publish, c, "x", "01234567890")
    assert(not ok and err:find("maximum payload"), err)
end)

test("invalid subjects rejected before hitting the wire", function()
    fake_server()
    local c = nats.client(); c:connect()
    assert(not pcall(c.publish, c, "bad subject", "x"))
    assert(not pcall(c.publish, c, "a..b", "x"))
    assert(not pcall(c.subscribe, c, "a.>.b"))
    assert(not pcall(c.subscribe, c, "a", nil, "bad queue"))
end)

test("unsubscribe with max auto-removes", function()
    local st = fake_server()
    local c = nats.client(); c:connect()
    local n = 0
    local sid = c:subscribe("m", function() n = n + 1 end)
    c:unsubscribe(sid, 2)
    assert(st.last:written():find("UNSUB " .. sid .. " 2\r\n", 1, true))
    st.last:feed(("MSG m " .. sid .. " 1\r\nx\r\n"):rep(2))
    c:wait({ maxcount = 2, timeout = 1 })
    eq(n, 2); eq(c.subscriptions[sid], nil)
end)

test("wait maxcount processes exactly maxcount messages", function()
    local st = fake_server()
    local c = nats.client(); c:connect()
    local n = 0
    local sid = c:subscribe("m", function() n = n + 1 end)
    st.last:feed(("MSG m " .. sid .. " 1\r\nx\r\n"):rep(3))
    c:wait({ maxcount = 2, timeout = 1 })
    eq(n, 2)
end)

test("INFO update: connect_urls merged, lame duck callback", function()
    local st = fake_server()
    local ldm = false
    local c = nats.client({ lame_duck_cb = function() ldm = true end }); c:connect()
    st.last:feed('INFO {"connect_urls":["10.1.1.1:4222","10.1.1.2:4222"],"ldm":true}\r\n')
    c:wait({ timeout = 0.05 })
    eq(ldm, true); eq(c.lame_duck, true)
    eq(#c.servers, 3)
end)

test("keepalive: missed PONGs -> reconnect + resubscribe", function()
    local st = fake_server()
    local events = {}
    local c = nats.client({ reconnect = true, ping_interval = 0.02, max_pings_out = 2, reconnect_wait = 0.01,
        disconnected_cb = function() events[#events + 1] = "down" end,
        reconnected_cb = function() events[#events + 1] = "up" end })
    c:connect()
    local sid = c:subscribe("keep.me", function() end, "q1")
    st.no_pong = true
    local first = st.last
    -- after the stale detection, a new socket is created; let it answer PONG
    local orig = nats.socket_factory
    nats.socket_factory = function(...) st.no_pong = false return orig(...) end
    c:wait({ timeout = 0.3 })
    nats.socket_factory = orig
    assert(st.last ~= first, "new connection created")
    eq(events[1], "down"); eq(events[2], "up")
    assert(st.last:written():find("SUB keep.me q1 " .. sid .. "\r\n", 1, true), "resubscribed")
    eq(c.stats.reconnects, 1)
end)

test("socket closed -> reconnect; without reconnect -> error", function()
    local st = fake_server()
    local c = nats.client({ reconnect = true, reconnect_wait = 0.01 }); c:connect()
    st.last.eof = true
    c:wait({ timeout = 0.1 })
    eq(c.connected, true); eq(c.stats.reconnects, 1)
    local c2 = nats.client(); c2:connect()
    st.last.eof = true
    assert(not pcall(c2.wait, c2, { timeout = 0.1 }))
end)

test("reconnect gives up after max_reconnect", function()
    local st = fake_server({ refuse = function(_, n) return n >= 1 end })
    local closed = false
    local c = nats.client({ reconnect = true, max_reconnect = 2, reconnect_wait = 0.001,
        closed_cb = function() closed = true end })
    c:connect()
    st.last.eof = true
    local ok, err = pcall(c.wait, c, { timeout = 1 })
    assert(not ok and tostring(err):find("reconnect failed"), tostring(err))
    eq(closed, true)
end)

test("flush and drain", function()
    local st = fake_server()
    local c = nats.client(); c:connect()
    eq(c:flush(1), true)
    local n = 0
    local sid = c:subscribe("d", function() n = n + 1 end)
    st.last:feed("MSG d " .. sid .. " 1\r\nx\r\n")
    eq(c:drain(1), true)
    eq(n, 1, "in-flight message delivered during drain")
    assert(st.last:written():find("UNSUB " .. sid .. "\r\n", 1, true))
    eq(c.connected, false); eq(c.closed, true)
end)

test("encoded client JSON round trip", function()
    local st = fake_server({ on_pub = function(sock, args, payload)
        if args[2] == "json.svc" then
            local t = nats.json.decode(payload)
            local out = nats.json.encode({ sum = t.a + t.b })
            sock:feed("MSG " .. args[3] .. " 1 " .. #out .. "\r\n" .. out .. "\r\n")
        end
    end })
    local c = nats.client(); c:connect()
    local ec = c:encoded_client()
    local r = ec:get("json.svc", { a = 2, b = 3 })
    eq(r.data.sum, 5)
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

test("nkey auth uses signature_cb with server nonce", function()
    local info = {}; for k, v in pairs(INFO) do info[k] = v end
    info.nonce = "NONCE123"
    local st = fake_server({ info = info })
    local c = nats.client({ nkey = "UPUBKEY", signature_cb = function(n) return "sig(" .. n .. ")" end })
    c:connect()
    eq(st.connects[1].nkey, "UPUBKEY"); eq(st.connects[1].sig, "sig(NONCE123)")
end)

print(string.format("%s: %d passed, %d failed", jit and jit.version or _VERSION, passed, failed))
os.exit(failed == 0 and 0 or 1)
