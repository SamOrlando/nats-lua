-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Parser chunks, ownership, nested get, queue bounds and the optional owner.
-- No NATS server. usage: lua tests/test_runtime.lua  (from the repository root)
package.path = "./?.lua;./tests/?.lua;" .. package.path
local nats = require("nats")
local fakesock = require("fakesock")

local pass, fail = 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then pass = pass + 1; print("ok   " .. name)
    else fail = fail + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, m)
    if a ~= b then error((m or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end
end
local function has(s, part)
    if type(s) ~= "string" or not s:find(part, 1, true) then
        error("expected [" .. part .. "] in [" .. tostring(s) .. "]", 2)
    end
end

local function clock_runtime(yields)
    local clock = 1000
    local rt = {
        now = function() return clock end,
        sleep = function(seconds)
            if seconds and seconds > 0 then clock = clock + seconds end
            if yields then coroutine.yield("sleep") end
        end,
        current = nats.runtime.current,
        read_file = nats.runtime.read_file,
    }
    return rt
end

local function client(opts)
    opts = opts or {}
    if opts.ping_interval == nil then opts.ping_interval = 0 end
    if opts.reconnect == nil then opts.reconnect = false end
    if not opts.runtime then opts.runtime = clock_runtime(false) end
    return nats.client(opts)
end

local function attach(nc, data)
    local sock = fakesock.new(data or "")
    rawset(nc, "socket", sock)
    rawset(nc, "connected", true)
    rawset(nc, "last_ping", nats.now(nc))
    return sock
end

local function msg(subject, sid, data)
    return string.format("MSG %s %s %d\r\n%s\r\n", subject, sid, #data, data)
end

test("feed splits lines, bodies and batches", function()
    local ops, rest = nats.feed("")
    eq(#ops, 0); eq(rest, "")
    ops, rest = nats.feed(nil)
    eq(#ops, 0); eq(rest, "")
    ops, rest = nats.feed("PIN")
    eq(#ops, 0); eq(rest, "PIN")

    local ping = nats.feed("PING\r\n")
    eq(ping[1], nats.feed("PING\r\n")[1])
    eq(ping[1].op, "PING")
    local batch, left = nats.feed("PONG\r\n+OK\r\n")
    eq(batch[1].op, "PONG"); eq(batch[2].op, "+OK"); eq(left, "")
    eq(batch[1], nats.feed("PONG\r\n")[1])

    ops, rest = nats.feed("MSG greet 2 4\r\npi")
    eq(#ops, 0)
    ops, rest = nats.feed(rest .. "ng\r\n")
    eq(ops[1].data, "ping"); eq(ops[1].subject, "greet"); eq(ops[1].sid, "2"); eq(rest, "")

    ops, rest = nats.feed("MSG gre")
    ops, rest = nats.feed(rest .. "et 2 4\r\nping\r\nPING\r\n")
    eq(#ops, 2); eq(ops[1].data, "ping"); eq(ops[2].op, "PING"); eq(rest, "")

    local payload = "a\r\nb\0c"
    -- Lua 5.1 string.format("%s") stops at an embedded zero.
    ops, rest = nats.feed("MSG s 2 " .. #payload .. "\r\n" .. payload .. "\r\n")
    eq(ops[1].data, payload); eq(rest, "")

    ops = nats.feed("MSG s 2 0\r\n\r\n")
    eq(ops[1].data, "")

    local hdr = "NATS/1.0 503 No Responders\r\n\r\n"
    local size = #hdr + 1
    local wire = string.format("HMSG s 2 %d %d\r\n", #hdr, size) .. hdr .. "x\r\n"
    ops = nats.feed(wire)
    eq(ops[1].op, "HMSG"); eq(ops[1].status, "503"); eq(ops[1].description, "No Responders"); eq(ops[1].data, "x")

    ops = nats.feed('INFO {"server_id":"Z"}\r\n')
    eq(ops[1].op, "INFO"); eq(ops[1].data, '{"server_id":"Z"}')
    ops = nats.feed("-ERR 'Permissions Violation'\r\n")
    eq(ops[1].op, "-ERR"); has(ops[1].data, "Permissions Violation")
    ops = nats.feed("WAT\r\n")
    eq(ops[1].op, "-ERR"); has(ops[1].data, "unknown protocol")
end)

test("empty read, queued ops and the protocol cap", function()
    local nc = client()
    attach(nc, "")
    local op, err = nats.response_read(nc, 0)
    eq(op, nil); eq(err, "timeout")

    nc = client()
    attach(nc, "PING\r\nPONG\r\n")
    local _, _, op1 = nc:step(0)
    local _, _, op2 = nc:step(0)
    eq(op1.op, "PING"); eq(op2.op, "PONG")
    eq(op1, nats.feed("PING\r\n")[1])
    eq(nc.pongs_received, 1)

    nc = client()
    nc.server_information.max_payload = 10
    attach(nc, string.rep("B", 9000))
    op, err = nats.response_read(nc, 0)
    eq(op, nil); eq(err, "protocol buffer exceeded")
end)

test("connect reads INFO, consumes the handshake PONG, and keeps a later op", function()
    local old = nats.socket_factory
    local nc = client()
    local sock
    nats.socket_factory = function()
        sock = fakesock.new('INFO {"server_id":"Z","max_payload":1024}\r\nPONG\r\nPING\r\n')
        return sock
    end
    local ok, err = pcall(function() nc:connect() end)
    nats.socket_factory = old
    if not ok then error(err, 0) end
    eq(nc.connected, true)
    eq(nc.server_information.server_id, "Z")
    eq(nc.server_information.max_payload, 1024)
    -- The handshake PONG is consumed and the counter is reset. The PING after it stays buffered.
    eq(nc.pongs_received, 0)
    local _, _, op = nc:step(0)
    eq(op and op.op, "PING")
    has(sock:written(), "PONG\r\n")
end)

test("creds file is read through runtime.read_file", function()
    local old = nats.socket_factory
    local seen
    local text = table.concat({
        "-----BEGIN NATS USER JWT-----",
        "abc.def.ghi",
        "------END NATS USER JWT-----",
        "-----BEGIN USER NKEY SEED-----",
        "SUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "------END USER NKEY SEED-----",
        "",
    }, "\n")
    local nc = client({
        creds = "anywhere.creds",
        runtime = {
            now = function() return 1000 end,
            sleep = function() end,
            current = nats.runtime.current,
            read_file = function(path) seen = path; return text end,
        },
    })
    local sock
    nats.socket_factory = function()
        sock = fakesock.new('INFO {"server_id":"Z","max_payload":1024}\r\nPONG\r\n')
        return sock
    end
    local ok, err = pcall(function() nc:connect() end)
    nats.socket_factory = old
    if not ok then error(err, 0) end
    eq(seen, "anywhere.creds")
    has(sock:written(), "abc.def.ghi")
end)

test("a callback get receives the reply", function()
    local nc = client()
    local sock = attach(nc, msg("greet", "2", "ping"))
    sock.on_send = function(self, s)
        local reply = s:match("^PUB %S+ (%S+) ")
        if not reply then return end
        local body = "pong"
        self:feed(string.format("MSG %s 1 %d\r\n%s\r\n", reply, #body, body))
    end
    local got, gerr
    nc:subscribe("greet", function()
        local m, err = nc:get("svc", "q", nil, nil, 1)
        got, gerr = m and m.data, err
    end)
    local r = nc:wait({ maxcount = 1, timeout = 1 })
    eq(r and r.data, "ping")
    eq(got, "pong", tostring(gerr))
end)

test("a second coroutine cannot publish during wait", function()
    local nc = client()
    attach(nc, msg("greet", "2", "ping"))
    local hit
    local co = coroutine.create(function()
        local ok, err = pcall(function() nc:publish("a", "b") end)
        hit = ok and "no error" or err
    end)
    nc:subscribe("greet", function()
        local ok, err = coroutine.resume(co)
        if not ok then hit = err end
    end)
    nc:wait({ maxcount = 1, timeout = 1 })
    has(hit, "connection is in use")
end)

test("a failed write inside a callback is not a callback error", function()
    local reported = {}
    local nc = client({ error_cb = function(_, e) reported[#reported + 1] = e end })
    local sock = attach(nc, msg("greet", "2", "ping"))
    local saw = false
    nc:subscribe("greet", function()
        nc:publish("out", "x")
        saw = true
    end)
    sock.send = function() return nil, "broken pipe" end
    local r = nc:wait({ maxcount = 1, timeout = 1 })
    eq(r and r.data, "ping")
    eq(saw, true)
    local joined = table.concat(reported, "\n")
    has(joined, "connection error")
    if joined:find("subscription callback error", 1, true) then
        error("I/O error was relabeled: " .. joined, 0)
    end
    local ok, err = pcall(function() nc:publish("z", "z") end)
    eq(ok, false)
    has(err, "not connected")
end)

test("queue_subscribe drops the newest at max_chan_len", function()
    local errs = {}
    local nc = client({
        max_chan_len = 2,
        error_cb = function(_, e) errs[#errs + 1] = e end,
    })
    attach(nc, msg("q", "2", "a") .. msg("q", "2", "b") .. msg("q", "2", "c"))
    local sub = nc:queue_subscribe("q")
    nc:wait({ maxcount = 3, timeout = 1 })
    eq(nc.stats.dropped, 1)
    eq(errs[1], "subscription queue full")
    eq(sub:next(0).data, "a")
    eq(sub:next(0).data, "b")
    local third, terr = sub:next(0)
    eq(third, nil); eq(terr, "timeout")
    sub:stop()
    local _, cerr = sub:next(0)
    eq(cerr, "closed")
end)

test("start without runtime.spawn fails and wait stays the loop", function()
    local nc = client()
    local ok, err = pcall(function() nc:start() end)
    eq(ok, false)
    has(err, "runtime.spawn")
end)

test("background owner runs a queued publish and a foreign next", function()
    local owner
    local rt = clock_runtime(true)
    rt.poll_interval = 0.05
    rt.spawn = function(fn) owner = coroutine.create(fn) end
    rt.park = function(cmd)
        while not cmd.done do coroutine.yield("park") end
    end
    rt.resume = function(co)
        local ok, err = coroutine.resume(co)
        if not ok then error(err, 0) end
    end
    local nc = client({ runtime = rt })
    local sock = attach(nc, "")
    local sub = nc:queue_subscribe("q")
    eq(nc:start(), true)

    local published = false
    local pub = coroutine.create(function()
        nc:publish("t", "hi")
        published = true
    end)
    local ok, why = coroutine.resume(pub)
    if not ok then error(why, 0) end
    eq(why, "park")

    local n = 0
    while not published do
        n = n + 1
        if n > 8 then error("owner did not run the publish", 0) end
        if coroutine.status(owner) == "dead" then error("owner stopped", 0) end
        ok, why = coroutine.resume(owner)
        if not ok then error(why, 0) end
    end
    has(sock:written(), "PUB t ")

    local got
    local waiter = coroutine.create(function()
        local m, err = sub:next(0.1)
        got = { m, err }
    end)
    ok, why = coroutine.resume(waiter)
    if not ok then error(why, 0) end
    eq(why, "sleep")
    while not got do
        ok, why = coroutine.resume(waiter)
        if not ok then error(why, 0) end
    end
    eq(got[1], nil); eq(got[2], "timeout")

    nc:stop()
    n = 0
    while coroutine.status(owner) ~= "dead" do
        n = n + 1
        if n > 8 then error("owner did not stop", 0) end
        ok, why = coroutine.resume(owner)
        if not ok then error(why, 0) end
    end
end)

print(string.format("%s runtime: %d passed, %d failed", _VERSION, pass, fail))
os.exit(fail == 0 and 0 or 1)
