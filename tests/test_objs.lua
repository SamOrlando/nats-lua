-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Unit tests for nats-sha256.lua and the object store helpers (no network).
package.path = "./?.lua;./tests/?.lua;" .. package.path
local sha256 = require("nats-sha256")
local nats = require("nats-objs")
local objs = nats.objs

local passed, failed = 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then passed = passed + 1 else failed = failed + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, msg)
    if a ~= b then error((msg or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end
end
local function hex(raw) return (raw:gsub(".", function(c) return string.format("%02x", c:byte()) end)) end

test("sha256 NIST vectors (" .. sha256.backend .. ")", function()
    eq(sha256.hex(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    eq(sha256.hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
    eq(sha256.hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
    eq(sha256.hex("abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"),
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1")
    eq(sha256.hex(string.rep("a", 1000000)), "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0")
end)

test("sha256 incremental updates match one-shot", function()
    local data = {}
    for i = 1, 1000 do data[i] = string.char(i % 256) end
    data = table.concat(data)
    local want = sha256.digest(data)
    for _, step in ipairs({ 1, 3, 55, 56, 63, 64, 65, 127, 999 }) do
        local h = sha256.new()
        for i = 1, #data, step do h:update(data:sub(i, i + step - 1)) end
        eq(hex(h:final()), hex(want), "step " .. step)
    end
    -- padding boundaries
    for n = 54, 66 do
        eq(#sha256.digest(string.rep("x", n)), 32)
        eq(hex(sha256.new_lua():update(string.rep("x", n)):final()), hex(sha256.digest(string.rep("x", n))))
    end
end)

test("base64url (RFC 4648) with padding, round trip", function()
    local v = { [""] = "", f = "Zg==", fo = "Zm8=", foo = "Zm9v", foob = "Zm9vYg==", fooba = "Zm9vYmE=", foobar = "Zm9vYmFy" }
    for k, e in pairs(v) do eq(objs.b64url_encode(k), e); eq(objs.b64url_decode(e), k) end
    eq(objs.b64url_encode("\251\255\254"), "-__-")
    eq(objs.encode_name("my/obj name.txt"), "bXkvb2JqIG5hbWUudHh0")
    for n = 0, 40 do
        local s = {}
        for i = 1, n do s[i] = string.char((i * 37) % 256) end
        s = table.concat(s)
        eq(objs.b64url_decode(objs.b64url_encode(s)), s)
    end
    assert(not objs.b64url_decode("@@@@"))
end)

test("digest value format (ADR-20)", function()
    eq(objs.digest_value(sha256.digest("")), "SHA-256=47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=")
end)

test("chunker and reader re-chunking", function()
    local data = string.rep("0123456789", 30) -- 300 bytes
    local sizes = {}
    for c in objs.chunker(data, 128) do sizes[#sizes + 1] = #c end
    eq(#sizes, 3); eq(sizes[1], 128); eq(sizes[3], 44)
    local pieces, i = { "ab", "cdefg", "", "h", "ijklmnop" }, 0
    local r = objs._make_reader(function() i = i + 1 return pieces[i] end, 4)
    local out = {}
    for c in r do out[#out + 1] = c end
    eq(table.concat(out, "|"), "abcd|efgh|ijkl|mnop")
    eq(objs._make_reader(nil, 4)(), nil)
    local cnt = 0
    for _ in objs.chunker("", 10) do cnt = cnt + 1 end
    eq(cnt, 0)
end)

test("stream config (ADR-20)", function()
    local c = objs.stream_config({ bucket = "MY-STORE", description = "d", ttl = 60, compression = true,
        metadata = { owner = "infra" } })
    eq(c.name, "OBJ_MY-STORE"); eq(c.subjects[1], "$O.MY-STORE.C.>"); eq(c.subjects[2], "$O.MY-STORE.M.>")
    eq(c.discard, "new"); eq(c.allow_rollup_hdrs, true); eq(c.allow_direct, true)
    eq(c.max_age, 60 * nats.jetstream.NS); eq(c.max_bytes, -1); eq(c.compression, "s2"); eq(c.storage, "file")
    assert(nats.valid_object_store_name("a-b_C9") and not nats.valid_object_store_name("a.b"))
end)

print(string.format("%s [sha256=%s]: %d passed, %d failed", jit and jit.version or _VERSION, sha256.backend, passed, failed))
os.exit(failed == 0 and 0 or 1)
