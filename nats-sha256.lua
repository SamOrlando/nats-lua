-- nats-sha256.lua - SHA-256 for nats.lua (object store digests)
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Uses luaossl (openssl.digest) when installed, otherwise a pure Lua
-- implementation: native integer operators on Lua 5.3+, the `bit` library on
-- LuaJIT, `bit32` on Lua 5.2 and lookup tables on plain Lua 5.1.
-- Words are kept as non-negative numbers in [0, 2^32).
--
--   local sha256 = require('nats-sha256')
--   local h = sha256.new(); h:update('abc'); local raw = h:final()  -- 32 bytes
--   sha256.digest('abc'), sha256.hex('abc')

local floor, char, byte, format, concat = math.floor, string.char, string.byte, string.format, table.concat
local M32 = 4294967296

local M = {}

-- ---------------------------------------------------------------- bit ops
local band, bxor, impl
if _VERSION ~= 'Lua 5.1' and _VERSION ~= 'Lua 5.2' then
	-- 5.3+: compile with load() so 5.1/5.2 parsers never see the operators
	local f = load([[
		return function(a, b) return (a & b) end, function(a, b) return (a ~ b) end
	]])
	band, bxor = f()
	impl = 'lua-int'
elseif jit and pcall(require, 'bit') then
	local bit = require('bit')
	local bb, bx = bit.band, bit.bxor
	band = function(a, b) return bb(a, b) % M32 end
	bxor = function(a, b) return bx(a, b) % M32 end
	impl = 'luajit-bit'
elseif pcall(require, 'bit32') then
	local bit32 = require('bit32')
	band, bxor = bit32.band, bit32.bxor
	impl = 'lua-bit32'
else
	-- Lua 5.1: 8-bit lookup tables
	local AND, XOR = {}, {}
	for a = 0, 255 do
		for b = 0, 255 do
			local r_and, r_xor, bitv, x, y = 0, 0, 1, a, b
			for _ = 1, 8 do
				local xa, yb = x % 2, y % 2
				if xa == 1 and yb == 1 then r_and = r_and + bitv end
				if xa ~= yb then r_xor = r_xor + bitv end
				x, y, bitv = (x - xa) / 2, (y - yb) / 2, bitv * 2
			end
			AND[a * 256 + b] = r_and
			XOR[a * 256 + b] = r_xor
		end
	end
	local function op(T)
		return function(a, b)
			local r, m = 0, 1
			for _ = 1, 4 do
				local x, y = a % 256, b % 256
				r = r + T[x * 256 + y] * m
				a, b, m = (a - x) / 256, (b - y) / 256, m * 256
			end
			return r
		end
	end
	band, bxor = op(AND), op(XOR)
	impl = 'lua-tables'
end

local function ror(x, n) -- rotate right, arithmetic (exact in doubles)
	local p = 2 ^ n
	local lo = x % p
	return (x - lo) / p + lo * (M32 / p)
end
local function shr(x, n)
	local p = 2 ^ n
	return (x - x % p) / p
end

local K = {
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}

local W = {}
local compress
compress = function(H, s, i)
	for t = 1, 16 do
		local a, b, c, d = byte(s, i, i + 3)
		W[t] = ((a * 256 + b) * 256 + c) * 256 + d
		i = i + 4
	end
	for t = 17, 64 do
		local w15, w2 = W[t - 15], W[t - 2]
		local s0 = bxor(bxor(ror(w15, 7), ror(w15, 18)), shr(w15, 3))
		local s1 = bxor(bxor(ror(w2, 17), ror(w2, 19)), shr(w2, 10))
		W[t] = (W[t - 16] + s0 + W[t - 7] + s1) % M32
	end
	local a, b, c, d, e, f, g, h = H[1], H[2], H[3], H[4], H[5], H[6], H[7], H[8]
	for t = 1, 64 do
		local S1 = bxor(bxor(ror(e, 6), ror(e, 11)), ror(e, 25))
		local ch = bxor(band(e, f), band(M32 - 1 - e, g))
		local t1 = (h + S1 + ch + K[t] + W[t]) % M32
		local S0 = bxor(bxor(ror(a, 2), ror(a, 13)), ror(a, 22))
		local maj = bxor(bxor(band(a, b), band(a, c)), band(b, c))
		local t2 = (S0 + maj) % M32
		h, g, f, e, d, c, b, a = g, f, e, (d + t1) % M32, c, b, a, (t1 + t2) % M32
	end
	H[1], H[2], H[3], H[4] = (H[1] + a) % M32, (H[2] + b) % M32, (H[3] + c) % M32, (H[4] + d) % M32
	H[5], H[6], H[7], H[8] = (H[5] + e) % M32, (H[6] + f) % M32, (H[7] + g) % M32, (H[8] + h) % M32
end

-- Lua 5.3+: integer specialised compression function (about 4x faster)
if impl == 'lua-int' then
	compress = load([[
		local K, W, byte = ...
		return function(H, s, i)
			for t = 1, 16 do
				local a, b, c, d = byte(s, i, i + 3)
				W[t] = (a << 24) | (b << 16) | (c << 8) | d
				i = i + 4
			end
			for t = 17, 64 do
				local x, y = W[t - 15], W[t - 2]
				local s0 = ((x >> 7) | (x << 25)) ~ ((x >> 18) | (x << 14)) ~ (x >> 3)
				local s1 = ((y >> 17) | (y << 15)) ~ ((y >> 19) | (y << 13)) ~ (y >> 10)
				W[t] = (W[t - 16] + (s0 & 0xffffffff) + W[t - 7] + (s1 & 0xffffffff)) & 0xffffffff
			end
			local a, b, c, d, e, f, g, h = H[1], H[2], H[3], H[4], H[5], H[6], H[7], H[8]
			for t = 1, 64 do
				local S1 = (((e >> 6) | (e << 26)) ~ ((e >> 11) | (e << 21)) ~ ((e >> 25) | (e << 7))) & 0xffffffff
				local ch = (e & f) ~ (~e & g)
				local t1 = (h + S1 + (ch & 0xffffffff) + K[t] + W[t]) & 0xffffffff
				local S0 = (((a >> 2) | (a << 30)) ~ ((a >> 13) | (a << 19)) ~ ((a >> 22) | (a << 10))) & 0xffffffff
				local maj = (a & b) ~ (a & c) ~ (b & c)
				h, g, f, e, d, c, b, a = g, f, e, (d + t1) & 0xffffffff, c, b, a, (t1 + S0 + maj) & 0xffffffff
			end
			H[1] = (H[1] + a) & 0xffffffff; H[2] = (H[2] + b) & 0xffffffff
			H[3] = (H[3] + c) & 0xffffffff; H[4] = (H[4] + d) & 0xffffffff
			H[5] = (H[5] + e) & 0xffffffff; H[6] = (H[6] + f) & 0xffffffff
			H[7] = (H[7] + g) & 0xffffffff; H[8] = (H[8] + h) & 0xffffffff
		end
	]])(K, W, byte)
end

local function be32(x)
	local d = x % 256; x = (x - d) / 256
	local c = x % 256; x = (x - c) / 256
	local b = x % 256; x = (x - b) / 256
	return char(x, b, c, d)
end

local Lua = {}
Lua.__index = Lua

local function new_lua()
	return setmetatable({
		H = { 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19 },
		buf = '', len = 0,
	}, Lua)
end

function Lua:update(s)
	s = tostring(s)
	self.len = self.len + #s
	local buf = self.buf
	if #buf > 0 then
		local need = 64 - #buf
		if #s < need then self.buf = buf .. s return self end
		compress(self.H, buf .. s:sub(1, need), 1)
		s = s:sub(need + 1)
	end
	local n = #s - #s % 64
	local H = self.H
	for i = 1, n, 64 do compress(H, s, i) end
	self.buf = s:sub(n + 1)
	return self
end

function Lua:final()
	local bits = self.len * 8
	local tail = self.buf .. '\128' .. string.rep('\0', (55 - #self.buf) % 64)
	local hi = floor(bits / M32)
	tail = tail .. be32(hi) .. be32(bits % M32)
	for i = 1, #tail, 64 do compress(self.H, tail, i) end
	local out = {}
	for i = 1, 8 do out[i] = be32(self.H[i]) end
	return concat(out)
end

-- optional native backend
local ok_ossl, odigest = pcall(require, 'openssl.digest')
if ok_ossl and type(odigest) == 'table' and odigest.new then
	M.new = function()
		local d = odigest.new('sha256')
		return { update = function(self, s) d:update(s) return self end, final = function() return d:final() end }
	end
	M.backend = 'luaossl'
else
	M.new = new_lua
	M.backend = impl
end
M.new_lua = new_lua

function M.digest(s) return M.new():update(s):final() end
function M.hex(s)
	return (M.digest(s):gsub('.', function(c) return format('%02x', byte(c)) end))
end

return M
