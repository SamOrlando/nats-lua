-- Micro-benchmarks for the parser and publish hot paths over a real loopback
-- LuaSocket connection (a helper process feeds/sinks the bytes; no NATS server).
-- usage: lua tests/bench.lua [N]      (NATS_MODULE=<module> to compare builds)
package.path = "./?.lua;./tests/?.lua;" .. package.path
local modname = os.getenv("NATS_MODULE") or "nats"
local nats = require(modname)
local socket = require("socket")
local gettime = socket.gettime
local N = tonumber(arg and arg[1]) or 200000
local lua = arg[-1] or "lua"
local port = 40000 + math.random(1, 20000)

local function feeder(kind)
    port = port + 1
    os.execute(string.format("%s tests/bench_feeder.lua %d %s %d &", lua, port, kind, N))
    for _ = 1, 50 do
        socket.sleep(0.05)
        local s = socket.tcp()
        if s:connect("127.0.0.1", port) then return s end
        s:close()
    end
    error("feeder did not start")
end

local function client_with(sock)
    local c = nats.client()
    rawset(c, "socket", sock)
    rawset(c, "connected", true)
    return c
end

local function bench_parse(kind)
    local sock = feeder(kind)
    local c = client_with(sock)
    local n = 0
    c.subscriptions["7"] = { subject = "bench.subject", callback = function() n = n + 1 end }
    local t0 = gettime()
    c:wait({ maxcount = (modname == "nats_old") and N + 1 or N, timeout = 30 })
    local dt = gettime() - t0
    sock:close()
    assert(n == N, kind .. " delivered " .. n)
    print(string.format("%-5s parse+dispatch: %9.0f msgs/s", kind, N / dt))
end

local function bench_publish(hdr)
    local sock = feeder("SINK")
    local c = client_with(sock)
    local payload = string.rep("x", 64)
    local header = hdr and "X-Id: 1\r\n" or nil
    local t0 = gettime()
    for _ = 1, N do c:publish("bench.subject", payload, nil, header) end
    local dt = gettime() - t0
    sock:close()
    print(string.format("%-5s publish:        %9.0f msgs/s", hdr and "HPUB" or "PUB", N / dt))
end

print((jit and jit.version or _VERSION) .. "  N=" .. N .. "  module=" .. modname)
bench_parse("MSG")
bench_parse("HMSG")
bench_publish(false)
bench_publish(true)
