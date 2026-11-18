-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Cross-check and benchmark nats-nkeys against narayana/lua-nkeys (Go nkeys).
-- nats-nkeys uses luasodium or luatweetnacl for ed25519 when that module loads.
-- lua-nkeys is optional and is not part of tests/run.sh.
--
-- usage: lua tests/bench_nkeys.lua [path/to/nkeys.so]
--        The .so directory is prepended to package.cpath. Without an argument,
--        require('nkeys') uses the default cpath.
package.path = "./?.lua;./tests/?.lua;" .. package.path

local so = arg and arg[1]
if so and so ~= "" then
    local dir = so:match("^(.*)/[^/]+%.so$") or so:gsub("/+$", "")
    package.cpath = dir .. "/?.so;" .. package.cpath
end

local nk = require("nats-nkeys")
local ok_gn, gn = pcall(require, "nkeys")
if not ok_gn then
    io.stderr:write("lua-nkeys is not loaded (" .. tostring(gn) .. ")\n")
    io.stderr:write("usage: lua tests/bench_nkeys.lua [path/to/nkeys.so]\n")
    os.exit(2)
end
local socket = require("socket")

local pass, fail = 0, 0
local function check(name, ok, detail)
    if ok then
        pass = pass + 1
        print("ok   " .. name)
    else
        fail = fail + 1
        print("FAIL " .. name .. (detail and (": " .. detail) or ""))
    end
end

local function hex(s)
    return (s:gsub(".", function(c) return string.format("%02x", c:byte()) end))
end

local KINDS = {
    { "user", gn.PREFIX_BYTE_USER },
    { "account", gn.PREFIX_BYTE_ACCOUNT },
    { "operator", gn.PREFIX_BYTE_OPERATOR },
    { "server", gn.PREFIX_BYTE_SERVER },
    { "cluster", gn.PREFIX_BYTE_CLUSTER },
}

print(string.format("runtime %s  nats-nkeys backend=%s  lua-nkeys prefixes user=%s account=%s",
    jit and jit.version or _VERSION, nk.backend or "lua",
    tostring(gn.PREFIX_BYTE_USER), tostring(gn.PREFIX_BYTE_ACCOUNT)))

check("prefix bytes match nats-nkeys",
    gn.PREFIX_BYTE_USER == nk.prefix.user
        and gn.PREFIX_BYTE_ACCOUNT == nk.prefix.account
        and gn.PREFIX_BYTE_OPERATOR == nk.prefix.operator
        and gn.PREFIX_BYTE_SERVER == nk.prefix.server
        and gn.PREFIX_BYTE_CLUSTER == nk.prefix.cluster,
    string.format("go u=%s a=%s o=%s n=%s c=%s",
        gn.PREFIX_BYTE_USER, gn.PREFIX_BYTE_ACCOUNT, gn.PREFIX_BYTE_OPERATOR,
        gn.PREFIX_BYTE_SERVER, gn.PREFIX_BYTE_CLUSTER))

local VECTOR_SEED = "SUANU6Z2W3GZXAGW7HW542S7DV64FPWMCN3DXKNMJU5DLTONOQDP4UFKNY"
local VECTOR_PUB = "UDHCE3WRB3YPXVFQR34XLEURROUWO22W377GUOHCY7SCO2DBLWH7QSJT"
local VECTOR_SIG = nk.unhex("818c77af18d3a12577df51f33a3e7c3f3d2e460d7bccca905f742dda56465253863b02d876352be79123da186eeb7cce55b562b9df568a94206707f52c648b0a")

local function sig_of(which, seed, msg)
    if which == "nk" then
        local kp, err = nk.from_seed(seed)
        if not kp then return nil, err end
        return kp:sign(msg), kp:public_key()
    end
    local kp, err = gn.from_seed(seed)
    if not kp then return nil, err end
    local sig, serr = gn.sign(kp, msg)
    if not sig then return nil, serr end
    local pub, perr = gn.public_key(kp)
    if not pub then return nil, perr end
    return sig, pub
end

do
    local nsig, npub = sig_of("nk", VECTOR_SEED, "hello")
    local gsig, gpub = sig_of("gn", VECTOR_SEED, "hello")
    check("Go vector public key", npub == VECTOR_PUB and gpub == VECTOR_PUB,
        string.format("nk=%s gn=%s", tostring(npub), tostring(gpub)))
    check("Go vector signature bytes", nsig == VECTOR_SIG and gsig == VECTOR_SIG,
        string.format("nk=%s gn=%s", nsig and hex(nsig) or "nil", gsig and hex(gsig) or "nil"))
    local ok, err = gn.verify(VECTOR_PUB, "hello", nsig)
    check("lua-nkeys verifies the vector", ok == true and err == nil, tostring(err))
    local nok, nerr = nk.verify(VECTOR_PUB, "hello", gsig)
    check("nats-nkeys verifies the vector", nok == true, tostring(nerr))
    local pub = nk.from_public(VECTOR_PUB)
    check("from_public verifies the vector", pub ~= nil and pub:verify("hello", nsig) == true,
        pub and select(2, pub:verify("hello", nsig)) or "no public key")
    local bad = nsig:sub(1, -2) .. string.char((nsig:byte(-1) + 1) % 256)
    local vok, verr = gn.verify(VECTOR_PUB, "hello", bad)
    check("lua-nkeys rejects a flipped signature", vok == false and verr ~= nil, tostring(verr))
    local _, nbad = nk.verify(VECTOR_PUB, "hello", bad)
    check("nats-nkeys rejects a flipped signature", nbad == "signature verification failed", tostring(nbad))
end

local function roundtrip(label, seed, pub_expect)
    local nsig, npub = sig_of("nk", seed, "payload-" .. label)
    local gsig, gpub = sig_of("gn", seed, "payload-" .. label)
    if not nsig or not gsig then
        check(label .. " sign", false, tostring(nsig or gsig))
        return
    end
    local same = nsig == gsig and npub == gpub and (not pub_expect or npub == pub_expect)
    check(label .. " public key and signature match", same,
        string.format("pub nk=%s gn=%s sig %s", tostring(npub), tostring(gpub), same and "eq" or "differ"))
    local ok, err = gn.verify(npub, "payload-" .. label, nsig)
    check(label .. " lua-nkeys verifies nats-nkeys signature", ok == true, tostring(err))
    local nok, nerr = nk.verify(gpub, "payload-" .. label, gsig)
    check(label .. " nats-nkeys verifies lua-nkeys signature", nok == true, tostring(nerr))
    local pub = nk.from_public(npub)
    local pok, perr = pub and pub:verify("payload-" .. label, gsig)
    check(label .. " public pair verifies", pub ~= nil and pok == true, tostring(perr))
end

for _, kind in ipairs(KINDS) do
    local name, prefix = kind[1], kind[2]
    local kp, err = nk.create_pair(name)
    check("nats-nkeys create " .. name, kp ~= nil, tostring(err))
    if kp then roundtrip("nk->" .. name, kp:seed(), kp:public_key()) end
    local seed, pub, gerr = gn.create_pair(prefix)
    check("lua-nkeys create " .. name, seed ~= nil and pub ~= nil, tostring(gerr))
    if seed then roundtrip("gn->" .. name, seed, pub) end
end

do
    local _, nerr = nk.from_seed("not-a-seed")
    local _, gerr = gn.from_seed("not-a-seed")
    check("both reject a garbage seed", nerr ~= nil and gerr ~= nil,
        string.format("nk=%s gn=%s", tostring(nerr), tostring(gerr)))
    -- nats-nkeys trims surrounding whitespace. lua-nkeys 0.2.0 does not.
    local kp = assert(nk.create_pair("user"))
    local padded = "  " .. kp:seed() .. "\n"
    local nk2 = nk.from_seed(padded)
    local gk, ge = gn.from_seed(padded)
    check("whitespace around a seed", nk2 ~= nil and gk == nil,
        string.format("nats-nkeys accepts it (%s); lua-nkeys err=%s",
            nk2 and nk2:public_key() or "no", tostring(ge)))
end

do
    local c = nk.create_pair("curve")
    local _, gerr = gn.create_pair(nk.prefix.curve)
    check("curve keys are nats-nkeys only",
        c and c:public_key():sub(1, 1) == "X" and gerr ~= nil,
        tostring(gerr))
end

local function bench(fn, budget, min_n)
    fn()
    local n = 0
    local t0 = socket.gettime()
    repeat
        fn()
        n = n + 1
    until n >= min_n and (socket.gettime() - t0) >= budget
    local dt = socket.gettime() - t0
    return n, dt, n / dt
end

local function row(impl, n, dt)
    print(string.format("  %-28s %6d ops  %8.1f ms  %12.1f ops/s  %10.3f ms/op",
        impl, n, dt * 1000, n / dt, (dt / n) * 1000))
end

print("")
print("benchmark (wall clock, one warmup, then until 0.75s and at least 3 ops)")

local nonce = string.rep("n", 22)
local kilo = string.rep("k", 1024)
local fixed = assert(nk.from_seed(VECTOR_SEED))
local gfixed, gferr = gn.from_seed(VECTOR_SEED)
if not gfixed then error(gferr) end
local nonce_sig = fixed:sign(nonce)
local kilo_sig = fixed:sign(kilo)
local rawpk = fixed:raw_public()
local pubonly = assert(nk.from_public(VECTOR_PUB))

local function gn_create()
    local seed, pub, err = gn.create_pair(gn.PREFIX_BYTE_USER)
    if not seed then error(err) end
    return seed, pub
end

local function gn_load()
    local kp, err = gn.from_seed(VECTOR_SEED)
    if not kp then error(err) end
    local pub, perr = gn.public_key(kp)
    if not pub then error(perr) end
    return pub
end

-- public_key() caches the ed25519 derivation. Clear it so this row measures
-- that derivation. create_pair and from_seed do not derive it up front.
local function nk_public()
    fixed._pk, fixed._pub = nil, nil
    return fixed:public_key()
end

local jobs = {
    { "create user key",
        function() nk.create_pair("user") end,
        gn_create },
    { "create user key + public key",
        function() assert(nk.create_pair("user")):public_key() end,
        gn_create },
    { "from_seed",
        function() nk.from_seed(VECTOR_SEED) end,
        function()
            local kp, err = gn.from_seed(VECTOR_SEED)
            if not kp then error(err) end
        end },
    { "from_seed + public key",
        function() nk.from_seed(VECTOR_SEED):public_key() end,
        gn_load },
    { "public_key (uncached)",
        nk_public,
        function()
            local pub, err = gn.public_key(gfixed)
            if not pub then error(err) end
        end },
    { "sign 22-byte nonce",
        function() fixed:sign(nonce) end,
        function()
            local sig, err = gn.sign(gfixed, nonce)
            if not sig then error(err) end
        end },
    { "sign 1024-byte payload",
        function() fixed:sign(kilo) end,
        function()
            local sig, err = gn.sign(gfixed, kilo)
            if not sig then error(err) end
        end },
    { "from_public",
        function()
            local pub, err = nk.from_public(VECTOR_PUB)
            if not pub then error(err) end
        end },
    { "verify 22-byte nonce",
        function()
            local ok, err = pubonly:verify(nonce, nonce_sig)
            if ok ~= true then error(err or "verify failed") end
        end,
        function()
            local ok, err = gn.verify(VECTOR_PUB, nonce, nonce_sig)
            if not ok then error(err or "verify failed") end
        end },
    { "verify 1024-byte payload",
        function()
            local ok, err = pubonly:verify(kilo, kilo_sig)
            if ok ~= true then error(err or "verify failed") end
        end,
        function()
            local ok, err = gn.verify(VECTOR_PUB, kilo, kilo_sig)
            if not ok then error(err or "verify failed") end
        end },
    { "verify 22-byte nonce (lua port)",
        function()
            local ok, err = nk.ed25519_verify_lua(rawpk, nonce, nonce_sig)
            if ok ~= true then error(err or "verify failed") end
        end,
        nil, "nats-nkeys (lua)" },
}

local active = nk.backend == "lua" and "nats-nkeys" or ("nats-nkeys (" .. nk.backend .. ")")
for _, job in ipairs(jobs) do
    print(job[1])
    if job[2] then
        local n, dt = bench(job[2], 0.75, 3)
        row(job[4] or active, n, dt)
    end
    if job[3] then
        local n, dt = bench(job[3], 0.75, 3)
        row("lua-nkeys", n, dt)
    end
end

print(string.format("\n%s checks: %d passed, %d failed", jit and jit.version or _VERSION, pass, fail))
os.exit(fail == 0 and 0 or 1)
