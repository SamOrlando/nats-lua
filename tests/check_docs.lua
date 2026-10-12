-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Verifies that every exported function of every public module has an entry
-- (a markdown heading containing "<object>:<name>(" or "<object>.<name>(") in
-- docs/reference/<module>.md.
-- usage: lua tests/check_docs.lua   (from the repository root; lua-nuid on LUA_PATH)
package.path = "./?.lua;" .. package.path

-- functions that are exported for module internals/tests and documented as such
-- in the "Internal" sections are still checked; only these are exempt:
local EXEMPT = {
	["ctx:check"] = true, ["ctx:on_msg"] = true, ["ctx:pull"] = true, ["ctx:reset_pending"] = true,
	["ordered:config"] = true, ["ordered:create"] = true, ["ordered:track"] = true,
}

local function fnames(t)
	local r = {}
	if type(t) ~= "table" then return r end
	for k, v in pairs(t) do
		if type(k) == "string" and type(v) == "function" and k:sub(1, 1) ~= "_" then r[k] = true end
	end
	return r
end
local function snapshot(t) local s = {} for k in pairs(fnames(t)) do s[k] = true end return s end
local function diff(t, before)
	local r = {}
	for k in pairs(fnames(t)) do if not before[k] then r[#r + 1] = k end end
	return r
end
local function all(t) local r = {} for k in pairs(fnames(t)) do r[#r + 1] = k end return r end
local function mt(x) return type(x) == "table" and (x.methods or x) or {} end

-- static scan for classes that are not reachable from module tables
local function static(file, class)
	local f = assert(io.open(file, "rb"))
	local src = f:read("*a")
	f:close()
	local r, seen = {}, {}
	for name in src:gmatch("\nfunction " .. class .. "[:.]([%a][%w_]*)%s*%(") do
		if not seen[name] then seen[name] = true r[#r + 1] = name end
	end
	for name in src:gmatch("\n" .. class .. "%.([%a][%w_]*)%s*=") do
		if not seen[name] and name ~= "__index" then seen[name] = true r[#r + 1] = name end
	end
	return r
end

local required = {} -- doc file -> list of "label<sep>name"
local function need(doc, label, sep, names)
	required[doc] = required[doc] or {}
	for _, n in ipairs(names) do
		local key = label .. sep .. n
		if not EXEMPT[key] then required[doc][#required[doc] + 1] = { label = label, sep = sep, name = n } end
	end
end

-- core
local nats = require("nats")
local mods = { nats = snapshot(nats) }
local cmm, msgm = nats.client_metatable.methods, nats.msg_metatable.methods
local cm0, mm0 = snapshot(cmm), snapshot(msgm)
need("nats.md", "nats", ".", all(nats))
need("nats.md", "client", ":", all(cmm))
need("nats.md", "msg", ":", all(msgm))
need("nats.md", "eclient", ":", all(nats.encoded_client_metatable.methods))
need("nats.md", "nats.json", ".", all(nats.json))

-- module that extend nats / client / msg
local function ext(modname, doc)
	local n0, c0, m0 = snapshot(nats), snapshot(cmm), snapshot(msgm)
	require(modname)
	need(doc, "nats", ".", diff(nats, n0))
	need(doc, "client", ":", diff(cmm, c0))
	need(doc, "msg", ":", diff(msgm, m0))
end

ext("nats-jetstream", "nats-jetstream.md")
local J = nats.jetstream
need("nats-jetstream.md", "nats.jetstream", ".", all(J))
need("nats-jetstream.md", "stream", ":", all(J.stream_metatable.methods))
need("nats-jetstream.md", "consumer", ":", all(J.consumer_metatable.methods))
need("nats-jetstream.md", "ctx", ":", all(J.consume_context))
need("nats-jetstream.md", "ordered", ":", all(J.ordered_metatable))
need("nats-jetstream.md", "iter", ":", static("nats-jetstream.lua", "MsgIter"))
need("nats-jetstream.md", "push_sub", ":", static("nats-jetstream.lua", "PushSub"))
need("nats-jetstream.md", "ordered_ctx", ":", static("nats-jetstream.lua", "outer"))

ext("nats-kvs", "nats-kvs.md")
need("nats-kvs.md", "nats.kvs", ".", all(nats.kvs))
need("nats-kvs.md", "kv", ":", all(nats.kvs.metatable.methods))
need("nats-kvs.md", "watcher", ":", all(nats.kvs.watcher_metatable))

ext("nats-objs", "nats-objs.md")
need("nats-objs.md", "nats.objs", ".", all(nats.objs))
need("nats-objs.md", "obs", ":", all(nats.objs.metatable.methods))
need("nats-objs.md", "obj_watcher", ":", static("nats-objs.lua", "ObjWatcher"))

ext("nats-micro", "nats-micro.md")
local mi = nats.micro
need("nats-micro.md", "nats.micro", ".", all(mi))
need("nats-micro.md", "svc", ":", all(mi.service_metatable))
need("nats-micro.md", "group", ":", all(mi.group_metatable))
need("nats-micro.md", "req", ":", static("nats-micro.lua", "Request"))

local nk = require("nats-nkeys")
need("nats-nkeys.md", "nkeys", ".", all(nk))
need("nats-nkeys.md", "kp", ":", static("nats-nkeys.lua", "KP"))

local sha = require("nats-sha256")
need("nats-sha256.md", "sha256", ".", all(sha))
need("nats-sha256.md", "hasher", ":", static("nats-sha256.lua", "Lua"))

-- check
local function plain(s) return (s:gsub("[%^%$%(%)%%%.%[%]%*%+%-%?]", "%%%0")) end
local missing, total, docs = 0, 0, {}
local names = {}
for doc in pairs(required) do names[#names + 1] = doc end
table.sort(names)
for _, doc in ipairs(names) do
	local f = io.open("docs/reference/" .. doc, "rb")
	local text = f and f:read("*a") or ""
	if f then f:close() else print("MISSING FILE docs/reference/" .. doc) end
	local heads = {}
	for line in (text .. "\n"):gmatch("([^\n]*)\n") do
		if line:match("^#+%s") then heads[#heads + 1] = line end
	end
	local n = 0
	table.sort(required[doc], function(a, b) return a.label .. a.name < b.label .. b.name end)
	for _, r in ipairs(required[doc]) do
		total, n = total + 1, n + 1
		local pat = "[%s`/]" .. plain(r.label) .. "[:.]" .. plain(r.name) .. "%("
		local found = false
		for _, h in ipairs(heads) do if (" " .. h):find(pat) then found = true break end end
		if not found then
			missing = missing + 1
			print(("MISSING %s: %s%s%s()"):format(doc, r.label, r.sep, r.name))
		end
	end
	docs[#docs + 1] = ("%s=%d"):format(doc, n)
end
print("functions per doc: " .. table.concat(docs, " "))
print(("%s check_docs: %d exported functions, %d documented, %d missing"):format(_VERSION, total, total - missing, missing))
os.exit(missing == 0 and 0 or 1)
