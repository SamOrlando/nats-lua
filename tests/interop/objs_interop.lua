-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Object store interop with nats.go: Lua writes -> Go reads, Go writes -> Lua reads.
-- usage: (cd tests/interop && go build -o interop .) then
--        NATS_URL=nats://host:4222 lua tests/interop/objs_interop.lua
package.path = "./?.lua;./tests/?.lua;" .. package.path
local nats = require("nats-objs")
local nuid = require("nuid")
local URL = os.getenv("NATS_URL") or "nats://127.0.0.1:4222"
local GO = os.getenv("INTEROP_BIN") or "tests/interop/interop"
local BUCKET = "nats-lua-test-" .. nuid.next():sub(1, 10) .. "-interop"

local nc = nats.client({ servers = { URL }, timeout = 5 }); nc:connect()
local obs = assert(nc:create_object_store({ bucket = BUCKET, storage = "memory" }))
local ok_all = true
local function check(c, what) print((c and "ok   " or "FAIL ") .. what); ok_all = ok_all and c end
local function sh(cmd)
    local p = io.popen(cmd .. " 2>&1"); local out = p:read("*a"); p:close(); return out
end

local ok, err = pcall(function()
    local t = {}
    for i = 1, 200 * 1024 + 3 do t[i] = string.char((i * 7) % 256) end
    local data = table.concat(t)
    local info = assert(obs:put({ name = "from-lua", headers = { ["X-Lua"] = { "1" } } }, data, { chunk_size = 50000 }))
    local out = sh(GO .. " " .. URL .. " " .. BUCKET .. " read from-lua")
    io.write("     " .. out)
    check(out:find("GO_READ_OK", 1, true) and out:find("size=" .. #data, 1, true)
        and out:find(nats.objs.sha256.hex(data), 1, true) and out:find(info.digest, 1, true), "Go reads Lua object")

    out = sh(GO .. " " .. URL .. " " .. BUCKET .. " write")
    io.write("     " .. out)
    local gdigest = out:match("digest=(%S+)")
    local gsha = out:match("sha=(%x+)")
    local gd, gi = obs:get("from-go/data.bin")
    check(gd and #gd == 333333 and nats.objs.sha256.hex(gd) == gsha, "Lua reads Go object (digest verified)")
    check(gi and gi.digest == gdigest and gi.headers["X-Go"][1] == "yes" and gi.options.max_chunk_size == 40000
        and gi.chunks == 9, "Go ObjectInfo fields decoded")
    local ld = obs:get("go-link")
    check(ld == gd, "Lua follows Go-created link")
    assert(obs:add_link("lua-link", obs:get_info("from-lua")))
    out = sh(GO .. " " .. URL .. " " .. BUCKET .. " readlink lua-link")
    io.write("     " .. out)
    check(out:find("GO_LINK_OK size=" .. #data, 1, true), "Go follows Lua-created link")
    local list = obs:list()
    check(#list == 4, "list sees 4 entries")
end)
if not ok then check(false, tostring(err)) end
assert(nc:delete_object_store(BUCKET))
local names = nc:stream_names() or {}
local left = false
for _, n in ipairs(names) do if n:find(BUCKET, 1, true) then left = true end end
check(not left, "interop bucket deleted")
nc:close()
os.exit(ok_all and 0 or 1)
