-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Tests against throw-away local nats-server processes: nkey auth, creds/user JWT
-- auth (operator mode, ADR-14), WebSocket transport and publish buffering across a
-- server restart (ADR-40).
-- usage: NATS_SERVER_BIN=/path/to/nats-server [NKEYSCHECK_BIN=...] lua tests/test_local.lua
package.path = "./?.lua;./tests/?.lua;" .. package.path
local nats = require("nats")
local nkeys = require("nats-nkeys")
local socket = require("socket")

local BIN = os.getenv("NATS_SERVER_BIN") or "nats-server"
local NKC = os.getenv("NKEYSCHECK_BIN")
local DIR = os.tmpname(); os.remove(DIR); os.execute("mkdir -p " .. DIR)
local passed, failed, skipped = 0, 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then passed = passed + 1; print("ok   " .. name)
    elseif tostring(err):find("SKIP") then skipped = skipped + 1; print("skip " .. name .. ": " .. tostring(err))
    else failed = failed + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, msg) if a ~= b then error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end end
local function write(path, s) local f = assert(io.open(path, "w")); f:write(s); f:close() end
local function wait_port(port)
    for _ = 1, 100 do
        local s = socket.tcp(); s:settimeout(0.1)
        if s:connect("127.0.0.1", port) then s:close() return true end
        s:close(); socket.sleep(0.05)
    end
    error("server on port " .. port .. " did not start")
end
local servers = {}
local function start(name, conf, port)
    write(DIR .. "/" .. name .. ".conf", conf)
    os.execute(BIN .. " -c " .. DIR .. "/" .. name .. ".conf -P " .. DIR .. "/" .. name .. ".pid > " ..
        DIR .. "/" .. name .. ".log 2>&1 &")
    wait_port(port)
    servers[name] = true
end
local function stop(name)
    local f = io.open(DIR .. "/" .. name .. ".pid")
    if f then local pid = f:read("*l"); f:close(); os.execute("kill " .. pid .. " 2>/dev/null"); socket.sleep(0.3) end
    servers[name] = nil
end
local function cleanup()
    for n in pairs(servers) do stop(n) end
    if os.getenv("KEEP_LOGS") then print("logs in " .. DIR) else os.execute("rm -rf " .. DIR) end
end

if os.execute(BIN .. " --version > /dev/null 2>&1") ~= true and os.execute(BIN .. " --version > /dev/null 2>&1") ~= 0 then
    print("SKIP: nats-server not found (set NATS_SERVER_BIN)")
    os.exit(0)
end

local user = assert(nkeys.create_pair("user"))
start("nkey", string.format([[
port: 14222
websocket { port: 18080, no_tls: true }
authorization { users = [ { nkey: %s } ] }
]], user:public_key()), 14222)
wait_port(18080)

test("nkey auth with pure Lua ed25519 signature", function()
    local c = nats.client({ servers = { "nats://127.0.0.1:14222" }, nkey_seed = user:seed() })
    assert(c:connect())
    assert(c:flush(2))
    c:close()
    local bad = nats.client({ servers = { "nats://127.0.0.1:14222" }, nkey_seed = nkeys.create_pair("user"):seed() })
    local ok, err = pcall(bad.connect, bad)
    assert(not ok and tostring(err):lower():find("authorization"), tostring(err))
end)

test("websocket transport (ws://) pub/sub/request with TCP peer", function()
    local ws = nats.client({ servers = { "ws://127.0.0.1:18080" }, nkey_seed = user:seed() })
    assert(ws:connect())
    local tcp = nats.client({ servers = { "nats://127.0.0.1:14222" }, nkey_seed = user:seed() }); tcp:connect()
    tcp:subscribe("svc.up", function(m) m:respond(m.data:upper()) end)
    local got
    local sid = ws:subscribe("from.tcp", function(m) got = m.data end)
    ws:flush(); tcp:flush()
    local big = string.rep("w", 200000)
    tcp:publish("from.tcp", big)
    tcp:flush()
    ws:wait({ sid = sid, timeout = 2 })
    eq(got, big, "large payload over websocket")
    -- both clients live in this process: alternate pumping requester and responder
    local msg, r = ws:request("svc.up", nil, "hello"), nil
    for _ = 1, 100 do
        tcp:wait({ timeout = 0.02 })
        r = ws:wait({ lid = msg.lid, timeout = 0.02 })
        if r then break end
    end
    assert(r and r.data == "HELLO", "request over websocket")
    ws:close(); tcp:close()
end)

test("publish buffering across a server restart (ADR-40)", function()
    local ev = {}
    local c = nats.client({ servers = { "nats://127.0.0.1:14222" }, nkey_seed = user:seed(), reconnect = true,
        reconnect_wait = 0.1, reconnect_jitter = 0, max_reconnect = -1,
        disconnected_cb = function() ev[#ev + 1] = "down" end, reconnected_cb = function() ev[#ev + 1] = "up" end })
    c:connect()
    local got = {}
    c:subscribe("buf.>", function(m) got[#got + 1] = m.data end)
    c:flush()
    local conf = io.open(DIR .. "/nkey.conf"):read("*a")
    stop("nkey")
    c:wait({ timeout = 0.3 })
    eq(c:status_string(), "reconnecting")
    for i = 1, 5 do c:publish("buf." .. i, "m" .. i) end
    assert(c:buffered() > 0, "buffered while down")
    start("nkey", conf, 14222)
    local t = socket.gettime() + 5
    while #got < 5 and socket.gettime() < t do c:wait({ timeout = 0.1 }) end
    eq(#got, 5, "buffered publishes delivered after reconnect")
    eq(got[1], "m1"); eq(got[5], "m5")
    eq(ev[1], "down"); eq(ev[2], "up")
    c:close()
end)
stop("nkey")

test("creds auth with a pure Lua issued user JWT (ADR-14, operator mode)", function()
    if not NKC then error("SKIP: NKEYSCHECK_BIN not set (go build tests/interop/nkeys)") end
    local out = io.popen(NKC .. " operator " .. DIR):read("*a")
    assert(out:find("OPERATOR_OK"), out)
    local aseed = io.open(DIR .. "/account.seed"):read("*a")
    start("op", "port: 14223\ninclude operator.conf\n", 14223)
    local u = nkeys.create_pair("user")
    local jwt = assert(nkeys.issue_user_jwt(aseed, u:public_key(), { name = "lua-user" }))
    local creds = nkeys.format_creds(jwt, u:seed())
    write(DIR .. "/user.creds", creds)
    local c = nats.client({ servers = { "nats://127.0.0.1:14223" }, creds = DIR .. "/user.creds" })
    assert(c:connect()); assert(c:flush(2))
    local got
    local sid = c:subscribe("jwt.test", function(m) got = m.data end)
    c:publish("jwt.test", "ok"); c:wait({ sid = sid, timeout = 1 })
    eq(got, "ok")
    c:close()
    -- user JWT signed by a different account must be rejected
    local other = nkeys.create_pair("account")
    local bad = nats.client({ servers = { "nats://127.0.0.1:14223" },
        creds = nkeys.format_creds(assert(nkeys.issue_user_jwt(other, u:public_key())), u:seed()) })
    assert(not pcall(bad.connect, bad), "foreign account rejected")
    stop("op")
end)

cleanup()
print(string.format("%s [local]: %d passed, %d failed, %d skipped", jit and jit.version or _VERSION, passed, failed, skipped))
os.exit(failed == 0 and 0 or 1)
