-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- KV / ordered consumer / service API interop with nats.go (and the nats CLI when installed).
-- usage: (cd tests/interop && go build -o jsinterop/jsinterop ./jsinterop) then
--        NATS_URL=nats://host:4222 lua tests/interop/js_interop.lua
package.path = "./?.lua;./tests/?.lua;" .. package.path
local nats = require("nats-kvs")
require("nats-micro")
local nuid = require("nuid")
local socket = require("socket")
local URL = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"
local GO = os.getenv("JSINTEROP_BIN") or "tests/interop/jsinterop/jsinterop"
local NATS_CLI = os.getenv("NATS_CLI") or "nats"
local RID = nuid.next():sub(1, 10)
local BUCKET = "nats-lua-test-" .. RID .. "-kvi"
local STREAM = "nats-lua-test-" .. RID .. "-ord"
local SUBJ = "nats-lua.test." .. RID
local SVC = "nats-lua-test-" .. RID

local nc = nats.client({ servers = { URL }, timeout = 5 }); nc:connect()
local ok_all = true
local function check(c, what) print((c and "ok   " or "FAIL ") .. what); ok_all = ok_all and (c and true or false) end
local function sh(cmd) local p = io.popen(cmd .. " 2>&1"); local out = p:read("*a"); p:close(); return out end
local function bg(cmd, file) os.execute(cmd .. " > " .. file .. " 2>&1 &") end
local function slurp(file) local f = io.open(file); local s = f and f:read("*a") or ""; if f then f:close() end; return s end
local function pump(secs, until_fn)
    local t = socket.gettime() + secs
    while socket.gettime() < t do
        nc:wait({ timeout = 0.1 })
        if until_fn and until_fn() then break end
    end
end
local has_cli = sh("command -v " .. NATS_CLI) ~= ""
local cli = NATS_CLI .. " -s " .. URL

local kv, st
local ok, err = pcall(function()
    -- KV ---------------------------------------------------------------------
    kv = assert(nc:create_key_value({ bucket = BUCKET, history = 5, storage = "memory" }))
    kv:put("cfg.a", "1"); kv:put("cfg.a", "2"); kv:put("cfg.b", "x"); kv:put("go-deleted", "soon")
    local out = sh(GO .. " kv-read " .. URL .. " " .. BUCKET .. " cfg.a")
    io.write("     " .. out)
    check(out:find("KV_READ value=2 rev=2 keys=3 history=2", 1, true), "Go reads Lua KV (get/keys/history)")
    out = sh(GO .. " kv-write " .. URL .. " " .. BUCKET .. " from.go hello")
    io.write("     " .. out)
    local e = kv:get("from.go")
    check(e and e.value == "hello", "Lua reads Go KV put")
    local _, gerr, de = kv:get("go-deleted")
    check(gerr == "key not found" and de and de.op == "DEL", "Lua sees Go delete marker")
    local ks = kv:keys(); table.sort(ks)
    check(table.concat(ks, ",") == "cfg.a,cfg.b,from.go", "Lua keys after Go writes: " .. table.concat(ks, ","))
    out = sh(GO .. " kv-watch " .. URL .. " " .. BUCKET)
    io.write("     " .. out)
    local n, marker = 0, false
    local w = kv:watch_all(function(x) if x then n = n + 1 else marker = true end end)
    w:wait_initial(5); w:stop()
    check(out:find("KV_WATCH initial=" .. n, 1, true) and marker, "Go and Lua watchers agree on initial data (" .. n .. ")")
    if has_cli then
        out = sh(cli .. " kv get " .. BUCKET .. " cfg.b --raw")
        check(out:match("^x"), "nats CLI kv get reads Lua value")
    end

    -- ordered consumer -------------------------------------------------------
    st = assert(nc:stream({ name = STREAM, subjects = { SUBJ .. ".o.>" }, storage = "memory" }, "create"))
    for i = 1, 30 do nc:js_publish(SUBJ .. ".o." .. (i % 3), tostring(i)) end
    out = sh(GO .. " ordered " .. URL .. " " .. STREAM .. " " .. SUBJ .. ".o.1 10")
    io.write("     " .. out)
    local oc = st:ordered_consumer({ filter_subject = SUBJ .. ".o.1" })
    local got, last = 0, 0
    local ctx = oc:consume(function(m) got = got + 1; last = m:metafields().stream_seq end)
    pump(5, function() return got >= 10 end)
    ctx.stop()
    check(got == 10 and out:find("ORDERED n=10 last=" .. math.floor(last), 1, true),
        "Lua and Go ordered consumers deliver the same sequence (last=" .. math.floor(last) .. ")")

    -- service API ------------------------------------------------------------
    local svc = assert(nc:add_service({ name = SVC, version = "0.3.0", description = "lua service",
        metadata = { lang = "lua" } }))
    local g = svc:add_group(SUBJ .. ".svc")
    g:add_endpoint("echo", function(req) req:respond("lua:" .. (req.data or "")) end, { metadata = { kind = "echo" } })
    g:add_endpoint("fail", function(req) req:respond_error(418, "teapot") end)
    nc:flush()
    local tmp = os.tmpname()
    bg(GO .. " micro-discover " .. URL .. " " .. SVC, tmp)
    pump(3.5, function() return slurp(tmp):find("MICRO", 1, true) ~= nil end)
    out = slurp(tmp)
    io.write("     " .. out)
    check(out:find("MICRO ping=1 endpoints=2", 1, true), "nats.go micro decodes Lua PING/INFO/STATS")
    if has_cli then
        bg(cli .. " micro info " .. SVC, tmp)
        pump(4, function() return slurp(tmp):find("Endpoints", 1, true) ~= nil end)
        pump(0.5)
        out = slurp(tmp)
        check(out:find(SVC, 1, true) and out:find("echo", 1, true), "nats CLI `micro info` lists Lua service")
        bg(cli .. " req " .. SUBJ .. ".svc.echo hi --raw", tmp)
        pump(10, function() return slurp(tmp):find("lua:hi", 1, true) ~= nil end)
        check(slurp(tmp):find("lua:hi", 1, true), "nats CLI request to Lua endpoint: " .. slurp(tmp):sub(1, 200))
        bg(cli .. " micro stats " .. SVC .. " --json", tmp)
        pump(4, function() return slurp(tmp):find("num_requests", 1, true) ~= nil end)
        pump(0.5)
        check(slurp(tmp):find('"num_requests": 1', 1, true) or slurp(tmp):find('"num_requests":1', 1, true),
            "nats CLI `micro stats` sees the request")
    end
    os.remove(tmp)
    svc:stop()

    bg(GO .. " micro-serve " .. URL .. " " .. SVC .. "go 4", tmp)
    pump(1.5)
    local r = nc:get(SVC .. "go.echo", "abc")
    check(r and r.data == "ABC", "Lua calls Go service endpoint")
    local r2 = nc:get(SVC .. "go.echo", "fail")
    local code, desc = nats.micro.response_error(r2)
    check(code == 400 and desc == "bad input", "Lua decodes Go service error headers")
    local pings = nats.micro.ping(nc, SVC .. "go")
    local infos = nats.micro.info(nc, SVC .. "go")
    check(#pings == 1 and infos[1] and infos[1].endpoints[1].name == "echo", "Lua discovers Go service")
    os.remove(tmp)
end)
if not ok then check(false, "interop run: " .. tostring(err)) end
if kv then kv:destroy() end
if st then st:delete() end
nc:close()
print(ok_all and "INTEROP PASS" or "INTEROP FAIL")
os.exit(ok_all and 0 or 1)
