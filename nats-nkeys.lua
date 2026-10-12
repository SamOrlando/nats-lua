-- nats-nkeys.lua - NKEYS, ed25519 signing, creds and user JWTs for nats.lua
--
-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
--
-- Pure Lua (5.1 - 5.4, LuaJIT) implementation of:
--   * SHA-512 and SHA-512/256
--   * ed25519 key derivation and signing (port of the TweetNaCl reference)
--   * NKEY seed/public key encoding (base32 + crc16) used by NATS auth
--   * x25519 curve keys (xkeys) with xsalsa20-poly1305 seal/open ('xkv1', nkeys compatible)
--   * ADR-14 user JWT issuing (sign a user JWT with an account seed) and creds files
-- A native backend is used for ed25519 when available: luasodium
-- (crypto_sign_seed_keypair / crypto_sign_detached) or luatweetnacl.
--
--   local nkeys = require('nats-nkeys')
--   local kp = nkeys.from_seed('SUA...')
--   kp:public_key()           -- 'UA...'
--   kp:sign(nonce)            -- 64 byte raw signature
--   kp:sign_nonce(nonce)      -- base64url (no padding) signature for CONNECT

local floor, char, byte, format, concat, rep = math.floor, string.char, string.byte, string.format, table.concat,
	string.rep
local type, tostring, setmetatable, pcall, error = type, tostring, setmetatable, pcall, error

local sha256 = require('nats-sha256')
local band, bxor = sha256._band, sha256._bxor
local M32 = 4294967296

local M = { backend = 'lua' }

-- ------------------------------------------------------------------ SHA-512
local function hx(s) return tonumber(s:sub(1, 8), 16), tonumber(s:sub(9, 16), 16) end
local K = {}
do
	local ks = [[
428a2f98d728ae22 7137449123ef65cd b5c0fbcfec4d3b2f e9b5dba58189dbbc 3956c25bf348b538 59f111f1b605d019
923f82a4af194f9b ab1c5ed5da6d8118 d807aa98a3030242 12835b0145706fbe 243185be4ee4b28c 550c7dc3d5ffb4e2
72be5d74f27b896f 80deb1fe3b1696b1 9bdc06a725c71235 c19bf174cf692694 e49b69c19ef14ad2 efbe4786384f25e3
0fc19dc68b8cd5b5 240ca1cc77ac9c65 2de92c6f592b0275 4a7484aa6ea6e483 5cb0a9dcbd41fbd4 76f988da831153b5
983e5152ee66dfab a831c66d2db43210 b00327c898fb213f bf597fc7beef0ee4 c6e00bf33da88fc2 d5a79147930aa725
06ca6351e003826f 142929670a0e6e70 27b70a8546d22ffc 2e1b21385c26c926 4d2c6dfc5ac42aed 53380d139d95b3df
650a73548baf63de 766a0abb3c77b2a8 81c2c92e47edaee6 92722c851482353b a2bfe8a14cf10364 a81a664bbc423001
c24b8b70d0f89791 c76c51a30654be30 d192e819d6ef5218 d69906245565a910 f40e35855771202a 106aa07032bbd1b8
19a4c116b8d2d0c8 1e376c085141ab53 2748774cdf8eeb99 34b0bcb5e19b48a8 391c0cb3c5c95a63 4ed8aa4ae3418acb
5b9cca4f7763e373 682e6ff3d6b2b8a3 748f82ee5defb2fc 78a5636f43172f60 84c87814a1f0ab72 8cc702081a6439ec
90befffa23631e28 a4506cebde82bde9 bef9a3f7b2c67915 c67178f2e372532b ca273eceea26619c d186b8c721c0c207
eada7dd6cde0eb1e f57d4f7fee6ed178 06f067aa72176fba 0a637dc5a2c898a6 113f9804bef90dae 1b710b35131c471b
28db77f523047d84 32caab7b40c72493 3c9ebe0a15c9bebc 431d67c49c100d4c 4cc5d4becb3e42b6 597f299cfc657e2a
5fcb6fab3ad6faec 6c44198c4a475817]]
	for w in ks:gmatch('%x+') do local h, l = hx(w) K[#K + 1] = h K[#K + 1] = l end
end
local IV512, IV256 = {}, {}
for w in ([[6a09e667f3bcc908 bb67ae8584caa73b 3c6ef372fe94f82b a54ff53a5f1d36f1
	510e527fade682d1 9b05688c2b3e6c1f 1f83d9abfb41bd6b 5be0cd19137e2179]]):gmatch('%x+') do
	local h, l = hx(w) IV512[#IV512 + 1] = h IV512[#IV512 + 1] = l
end
for w in ([[22312194fc2bf72c 9f555fa3c84c64c2 2393b86b6f53b151 963877195940eabd
	96283ee2a88effe3 be5e1e2553863992 2b0199fc2c85b8aa 0eb72ddc81c52ca2]]):gmatch('%x+') do
	local h, l = hx(w) IV256[#IV256 + 1] = h IV256[#IV256 + 1] = l
end

local function rotr(h, l, n)
	if n >= 32 then h, l, n = l, h, n - 32 end
	if n == 0 then return h, l end
	local p, q = 2 ^ n, 2 ^ (32 - n)
	return floor(h / p) + (l % p) * q, floor(l / p) + (h % p) * q
end
local function shr(h, l, n)
	local p, q = 2 ^ n, 2 ^ (32 - n)
	return floor(h / p), floor(l / p) + (h % p) * q
end
local function x3(a, b, c) return bxor(bxor(a, b), c) end

local W = {}
local function compress512(H, s, i)
	for t = 1, 32 do
		local a, b, c, d = byte(s, i, i + 3)
		W[t] = ((a * 256 + b) * 256 + c) * 256 + d
		i = i + 4
	end
	for t = 33, 160, 2 do
		local h15, l15 = W[t - 30], W[t - 29]
		local h2, l2 = W[t - 4], W[t - 3]
		local ah, al = rotr(h15, l15, 1)
		local bh, bl = rotr(h15, l15, 8)
		local ch, cl = shr(h15, l15, 7)
		local s0h, s0l = x3(ah, bh, ch), x3(al, bl, cl)
		ah, al = rotr(h2, l2, 19)
		bh, bl = rotr(h2, l2, 61)
		ch, cl = shr(h2, l2, 6)
		local s1h, s1l = x3(ah, bh, ch), x3(al, bl, cl)
		local lo = W[t - 31] + s0l + W[t - 13] + s1l
		local carry = floor(lo / M32)
		W[t + 1] = lo % M32
		W[t] = (W[t - 32] + s0h + W[t - 14] + s1h + carry) % M32
	end
	local ah, al, bh, bl, chh, cl, dh, dl = H[1], H[2], H[3], H[4], H[5], H[6], H[7], H[8]
	local eh, el, fh, fl, gh, gl, hh, hl = H[9], H[10], H[11], H[12], H[13], H[14], H[15], H[16]
	for t = 1, 160, 2 do
		local r1h, r1l = rotr(eh, el, 14)
		local r2h, r2l = rotr(eh, el, 18)
		local r3h, r3l = rotr(eh, el, 41)
		local S1h, S1l = x3(r1h, r2h, r3h), x3(r1l, r2l, r3l)
		local chh2 = bxor(band(eh, fh), band(M32 - 1 - eh, gh))
		local chl2 = bxor(band(el, fl), band(M32 - 1 - el, gl))
		local lo = hl + S1l + chl2 + K[t + 1] + W[t + 1]
		local t1l = lo % M32
		local t1h = (hh + S1h + chh2 + K[t] + W[t] + floor(lo / M32)) % M32
		r1h, r1l = rotr(ah, al, 28)
		r2h, r2l = rotr(ah, al, 34)
		r3h, r3l = rotr(ah, al, 39)
		local S0h, S0l = x3(r1h, r2h, r3h), x3(r1l, r2l, r3l)
		local mh = x3(band(ah, bh), band(ah, chh), band(bh, chh))
		local ml = x3(band(al, bl), band(al, cl), band(bl, cl))
		local lo2 = S0l + ml
		local t2l = lo2 % M32
		local t2h = (S0h + mh + floor(lo2 / M32)) % M32
		hh, hl, gh, gl, fh, fl = gh, gl, fh, fl, eh, el
		lo = dl + t1l
		el = lo % M32
		eh = (dh + t1h + floor(lo / M32)) % M32
		dh, dl, chh, cl, bh, bl = chh, cl, bh, bl, ah, al
		lo = t1l + t2l
		al = lo % M32
		ah = (t1h + t2h + floor(lo / M32)) % M32
	end
	local v = { ah, al, bh, bl, chh, cl, dh, dl, eh, el, fh, fl, gh, gl, hh, hl }
	for k = 1, 16, 2 do
		local lo = H[k + 1] + v[k + 1]
		H[k + 1] = lo % M32
		H[k] = (H[k] + v[k] + floor(lo / M32)) % M32
	end
end

local function be32(x)
	local d = x % 256; x = (x - d) / 256
	local c = x % 256; x = (x - c) / 256
	local b = x % 256; x = (x - b) / 256
	return char(x, b, c, d)
end

local function sha512_raw(s, iv, outlen)
	local H = {}
	for k = 1, 16 do H[k] = iv[k] end
	local len = #s
	local n = len - len % 128
	for i = 1, n, 128 do compress512(H, s, i) end
	local tail = s:sub(n + 1) .. '\128'
	tail = tail .. rep('\0', (112 - #tail) % 128)
	local bits = len * 8
	tail = tail .. rep('\0', 8) .. be32(floor(bits / M32)) .. be32(bits % M32)
	for i = 1, #tail, 128 do compress512(H, tail, i) end
	local out = {}
	for k = 1, 16 do out[k] = be32(H[k]) end
	return concat(out):sub(1, outlen)
end
function M.sha512(s) return sha512_raw(s, IV512, 64) end
function M.sha512_256(s) return sha512_raw(s, IV256, 32) end

-- ------------------------------------------------------------------ ed25519 (TweetNaCl port)
-- field elements: 16 limbs of 16 bits, 0-indexed, plain Lua numbers (exact in doubles)
local function gf(init)
	local r = {}
	for i = 0, 15 do r[i] = init and init[i + 1] or 0 end
	return r
end
local gf0 = gf()
local gf1 = gf({ 1 })
local D2 = gf({ 0xf159, 0x26b2, 0x9b94, 0xebd6, 0xb156, 0x8283, 0x149a, 0x00e0,
	0xd130, 0xeef3, 0x80f2, 0x198e, 0xfce7, 0x56df, 0xd9dc, 0x2406 })
local GX = gf({ 0xd51a, 0x8f25, 0x2d60, 0xc956, 0xa7b2, 0x9525, 0xc760, 0x692c,
	0xdc5c, 0xfdd6, 0xe231, 0xc0a4, 0x53fe, 0xcd6e, 0x36d3, 0x2169 })
local GY = gf({ 0x6658, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666,
	0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666 })

local function set(r, a) for i = 0, 15 do r[i] = a[i] end end

local function car(o)
	for i = 0, 15 do
		local v = o[i] + 65536
		local c = floor(v / 65536)
		if i < 15 then o[i + 1] = o[i + 1] + c - 1 else o[0] = o[0] + 38 * (c - 1) end
		o[i] = v - c * 65536
	end
end

local function sel(p, q, b)
	if b == 1 then
		for i = 0, 15 do p[i], q[i] = q[i], p[i] end
	end
end

local function pack25519(n)
	local t, m = gf(), gf()
	set(t, n)
	car(t) car(t) car(t)
	for _ = 1, 2 do
		m[0] = t[0] - 0xffed
		for i = 1, 14 do
			local borrow = m[i - 1] < 0 and 1 or 0
			m[i] = t[i] - 0xffff - borrow
			m[i - 1] = m[i - 1] % 65536
		end
		local borrow = m[14] < 0 and 1 or 0
		m[15] = t[15] - 0x7fff - borrow
		local b = m[15] < 0 and 1 or 0
		m[14] = m[14] % 65536
		sel(t, m, 1 - b)
	end
	local o = {}
	for i = 0, 15 do
		local v = t[i]
		o[2 * i + 1] = char(v % 256)
		o[2 * i + 2] = char(floor(v / 256) % 256)
	end
	return concat(o)
end

local function A(o, a, b) for i = 0, 15 do o[i] = a[i] + b[i] end end
local function Z(o, a, b) for i = 0, 15 do o[i] = a[i] - b[i] end end
local T = {}
local function Mul(o, a, b)
	for i = 0, 30 do T[i] = 0 end
	for i = 0, 15 do
		local ai = a[i]
		if ai ~= 0 then
			for j = 0, 15 do T[i + j] = T[i + j] + ai * b[j] end
		end
	end
	for i = 0, 14 do T[i] = T[i] + 38 * T[i + 16] end
	for i = 0, 15 do o[i] = T[i] end
	car(o) car(o)
end
local function S(o, a) Mul(o, a, a) end

local function inv25519(o, i)
	local c = gf()
	set(c, i)
	for a = 253, 0, -1 do
		S(c, c)
		if a ~= 2 and a ~= 4 then Mul(c, c, i) end
	end
	set(o, c)
end

local function par25519(a) return byte(pack25519(a), 1) % 2 end

local ta, tb, tc, td, te, tf, tg, th, tt = gf(), gf(), gf(), gf(), gf(), gf(), gf(), gf(), gf()
local function add(p, q)
	Z(ta, p[1], p[0]); Z(tt, q[1], q[0]); Mul(ta, ta, tt)
	A(tb, p[0], p[1]); A(tt, q[0], q[1]); Mul(tb, tb, tt)
	Mul(tc, p[3], q[3]); Mul(tc, tc, D2)
	Mul(td, p[2], q[2]); A(td, td, td)
	Z(te, tb, ta); Z(tf, td, tc); A(tg, td, tc); A(th, tb, ta)
	Mul(p[0], te, tf); Mul(p[1], th, tg); Mul(p[2], tg, tf); Mul(p[3], te, th)
end

local function cswap(p, q, b) for i = 0, 3 do sel(p[i], q[i], b) end end

local function pack(p)
	local zi, tx, ty = gf(), gf(), gf()
	inv25519(zi, p[2])
	Mul(tx, p[0], zi)
	Mul(ty, p[1], zi)
	local r = pack25519(ty)
	local last = byte(r, 32) + par25519(tx) * 128
	return r:sub(1, 31) .. char(last)
end

local function scalarbase(s) -- s: 32 byte string
	local p = { [0] = gf(), [1] = gf(), [2] = gf(), [3] = gf() }
	set(p[0], gf0); set(p[1], gf1); set(p[2], gf1); set(p[3], gf0)
	local q = { [0] = gf(), [1] = gf(), [2] = gf(), [3] = gf() }
	set(q[0], GX); set(q[1], GY); set(q[2], gf1); Mul(q[3], GX, GY)
	for i = 255, 0, -1 do
		local b = floor(byte(s, floor(i / 8) + 1) / 2 ^ (i % 8)) % 2
		cswap(p, q, b)
		add(q, p)
		add(p, p)
		cswap(p, q, b)
	end
	return p
end

local L = { [0] = 0xed, 0xd3, 0xf5, 0x5c, 0x1a, 0x63, 0x12, 0x58, 0xd6, 0x9c, 0xf7, 0xa2, 0xde, 0xf9, 0xde, 0x14,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10 }

local function modL(x) -- x: 0-indexed table of 64 numbers; returns 32 byte string
	for i = 63, 32, -1 do
		local carry = 0
		local j = i - 32
		while j < i - 12 do
			x[j] = x[j] + carry - 16 * x[i] * L[j - (i - 32)]
			carry = floor((x[j] + 128) / 256)
			x[j] = x[j] - carry * 256
			j = j + 1
		end
		x[j] = x[j] + carry
		x[i] = 0
	end
	local carry = 0
	for j = 0, 31 do
		x[j] = x[j] + carry - floor(x[31] / 16) * L[j]
		carry = floor(x[j] / 256)
		x[j] = x[j] % 256
	end
	for j = 0, 31 do x[j] = x[j] - carry * L[j] end
	local r = {}
	for i = 0, 31 do
		x[i + 1] = (x[i + 1] or 0) + floor(x[i] / 256)
		r[i + 1] = char(x[i] % 256)
	end
	return concat(r)
end

local function reduce(h) -- 64 byte string -> 32 byte scalar
	local x = {}
	for i = 0, 63 do x[i] = byte(h, i + 1) end
	return modL(x)
end

local function expand(seed)
	local d = M.sha512(seed)
	local b0, b31 = byte(d, 1), byte(d, 32)
	b0 = b0 - b0 % 8
	b31 = b31 % 128
	if b31 < 64 then b31 = b31 + 64 end
	local a = char(b0) .. d:sub(2, 31) .. char(b31)
	return a, d:sub(33, 64)
end

local function lua_public(seed)
	local a = expand(seed)
	return pack(scalarbase(a))
end

local function lua_sign(seed, msg, pk)
	local a, prefix = expand(seed)
	pk = pk or pack(scalarbase(a))
	local r = reduce(M.sha512(prefix .. msg))
	local R = pack(scalarbase(r))
	local h = reduce(M.sha512(R .. pk .. msg))
	local x = {}
	for i = 0, 63 do x[i] = 0 end
	for i = 0, 31 do x[i] = byte(r, i + 1) end
	for i = 0, 31 do
		local hi = byte(h, i + 1)
		for j = 0, 31 do x[i + j] = x[i + j] + hi * byte(a, j + 1) end
	end
	return R .. modL(x)
end

M.ed25519_public_lua, M.ed25519_sign_lua = lua_public, lua_sign
local ed_public, ed_sign = lua_public, lua_sign

-- optional native backends
do
	local ok, sodium = pcall(require, 'luasodium')
	if ok and type(sodium) == 'table' and sodium.crypto_sign_seed_keypair and sodium.crypto_sign_detached then
		ed_public = function(seed) local pk = sodium.crypto_sign_seed_keypair(seed) return pk end
		ed_sign = function(seed, msg)
			local _, sk = sodium.crypto_sign_seed_keypair(seed)
			return sodium.crypto_sign_detached(msg, sk)
		end
		M.backend = 'luasodium'
	else
		local ok2, nacl = pcall(require, 'luatweetnacl')
		if ok2 and type(nacl) == 'table' and nacl.sign_keypair and nacl.sign then
			ed_public = function(seed) local pk = nacl.sign_keypair(seed) return pk end
			ed_sign = function(seed, msg)
				local _, sk = nacl.sign_keypair(seed)
				return nacl.sign(msg, sk):sub(1, 64)
			end
			M.backend = 'luatweetnacl'
		end
	end
end
function M.ed25519_public(seed) return ed_public(seed) end
function M.ed25519_sign(seed, msg, pk) return ed_sign(seed, msg, pk) end

-- ------------------------------------------------------------------ x25519 / xsalsa20-poly1305 (xkeys)
-- TweetNaCl crypto_scalarmult, crypto_box (xsalsa20 + poly1305), pure Lua.
local function unpack25519(n)
	local o = gf()
	for i = 0, 15 do o[i] = byte(n, 2 * i + 1) + byte(n, 2 * i + 2) * 256 end
	o[15] = o[15] % 32768
	return o
end
local G121665 = gf({ 0xDB41, 1 })
local BASE9 = char(9) .. rep('\0', 31)

local function x25519(n, p) -- n: 32 byte scalar, p: 32 byte u coordinate -> 32 bytes
	local z = { byte(n, 1, 32) }
	z[32] = z[32] % 64 + 64 -- (b & 127) | 64
	z[1] = z[1] - z[1] % 8
	local x = unpack25519(p)
	local a, b, c, d, e, f = gf(), gf(), gf(), gf(), gf(), gf()
	set(b, x)
	a[0], d[0] = 1, 1
	for i = 254, 0, -1 do
		local r = floor(z[floor(i / 8) + 1] / 2 ^ (i % 8)) % 2
		sel(a, b, r) sel(c, d, r)
		A(e, a, c) Z(a, a, c) A(c, b, d) Z(b, b, d)
		S(d, e) S(f, a) Mul(a, c, a) Mul(c, b, e)
		A(e, a, c) Z(a, a, c) S(b, a) Z(c, d, f)
		Mul(a, c, G121665) A(a, a, d) Mul(c, c, a) Mul(a, d, f) Mul(d, b, x) S(b, e)
		sel(a, b, r) sel(c, d, r)
	end
	inv25519(c, c)
	Mul(a, a, c)
	return pack25519(a)
end
M.x25519 = x25519
function M.x25519_base(n) return x25519(n, BASE9) end

-- salsa20 core (h = true: hsalsa20 -> 32 bytes, else 64 byte block)
local function ld32(s, i) local a, b, c, d = byte(s, i, i + 3) return a + b * 256 + c * 65536 + d * 16777216 end
local function st32(v) return char(v % 256, floor(v / 256) % 256, floor(v / 65536) % 256, floor(v / 16777216) % 256) end
local function L32(x, c) x = x % M32 local hi = floor(x / 2 ^ (32 - c)) return (x * 2 ^ c) % M32 + hi end
local SIGMA = 'expand 32-byte k'
local function core(inp, k, h)
	local x, y, w, t = {}, {}, {}, {}
	for i = 0, 3 do
		x[5 * i] = ld32(SIGMA, 4 * i + 1)
		x[1 + i] = ld32(k, 4 * i + 1)
		x[6 + i] = ld32(inp, 4 * i + 1)
		x[11 + i] = ld32(k, 16 + 4 * i + 1)
	end
	for i = 0, 15 do y[i] = x[i] end
	for _ = 1, 20 do
		for j = 0, 3 do
			for m = 0, 3 do t[m] = x[(5 * j + 4 * m) % 16] end
			t[1] = bxor(t[1], L32(t[0] + t[3], 7))
			t[2] = bxor(t[2], L32(t[1] + t[0], 9))
			t[3] = bxor(t[3], L32(t[2] + t[1], 13))
			t[0] = bxor(t[0], L32(t[3] + t[2], 18))
			for m = 0, 3 do w[4 * j + (j + m) % 4] = t[m] end
		end
		for m = 0, 15 do x[m] = w[m] end
	end
	local out = {}
	if h then
		for i = 0, 3 do out[#out + 1] = st32(x[5 * i]) end
		for i = 0, 3 do out[#out + 1] = st32(x[6 + i]) end
	else
		for i = 0, 15 do out[#out + 1] = st32((x[i] + y[i]) % M32) end
	end
	return concat(out)
end
local function hsalsa20(inp16, k) return core(inp16, k, true) end
M.hsalsa20 = hsalsa20

-- salsa20 stream with 8 byte nonce xored with m (counter from 0)
local function salsa20_xor(m, n8, k)
	local out, ctr, pos = {}, 0, 1
	while pos <= #m do
		local cb = {}
		local c = ctr
		for i = 1, 8 do cb[i] = char(c % 256) c = floor(c / 256) end
		local ks = core(n8 .. concat(cb), k, false)
		local chunk = m:sub(pos, pos + 63)
		local o = {}
		for i = 1, #chunk do o[i] = char(bxor(byte(chunk, i), byte(ks, i))) end
		out[#out + 1] = concat(o)
		pos, ctr = pos + 64, ctr + 1
	end
	return concat(out)
end
local function xsalsa20_xor(m, n24, k) return salsa20_xor(m, n24:sub(17, 24), hsalsa20(n24:sub(1, 16), k)) end
M.xsalsa20_xor = xsalsa20_xor

local MINUSP = { [0] = 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 252 }
local function add1305(h, c)
	local u = 0
	for j = 0, 16 do
		u = u + h[j] + c[j]
		h[j] = u % 256
		u = floor(u / 256)
	end
end
local function poly1305(m, k) -- 16 byte tag
	local r, h, c, x, g = {}, {}, {}, {}, {}
	for j = 0, 16 do r[j], h[j] = 0, 0 end
	for j = 0, 15 do r[j] = byte(k, j + 1) end
	r[3] = r[3] % 16; r[4] = r[4] - r[4] % 4; r[7] = r[7] % 16; r[8] = r[8] - r[8] % 4
	r[11] = r[11] % 16; r[12] = r[12] - r[12] % 4; r[15] = r[15] % 16
	local pos, n = 1, #m
	while n > 0 do
		for j = 0, 16 do c[j] = 0 end
		local j = 0
		while j < 16 and j < n do c[j] = byte(m, pos + j) j = j + 1 end
		c[j] = 1
		pos, n = pos + j, n - j
		add1305(h, c)
		for i = 0, 16 do
			local v = 0
			for jj = 0, 16 do v = v + h[jj] * ((jj <= i) and r[i - jj] or 320 * r[i + 17 - jj]) end
			x[i] = v
		end
		for i = 0, 16 do h[i] = x[i] end
		local u = 0
		for jj = 0, 15 do u = u + h[jj] h[jj] = u % 256 u = floor(u / 256) end
		u = u + h[16] h[16] = u % 4
		u = 5 * floor(u / 4)
		for jj = 0, 15 do u = u + h[jj] h[jj] = u % 256 u = floor(u / 256) end
		u = u + h[16] h[16] = u
	end
	for j = 0, 16 do g[j] = h[j] end
	add1305(h, MINUSP)
	if floor(h[16] / 128) % 2 == 1 then for j = 0, 16 do h[j] = g[j] end end
	for j = 0, 15 do c[j] = byte(k, j + 17) end
	c[16] = 0
	add1305(h, c)
	local o = {}
	for j = 0, 15 do o[j + 1] = char(h[j]) end
	return concat(o)
end
M.poly1305 = poly1305

local ZERO16, ZERO32 = rep('\0', 16), rep('\0', 32)
local function box_key(pk, sk) return hsalsa20(ZERO16, x25519(sk, pk)) end
-- nacl secretbox: tag(16) .. ciphertext
local function secretbox(m, n24, k)
	local c = xsalsa20_xor(ZERO32 .. m, n24, k)
	return poly1305(c:sub(33), c:sub(1, 32)) .. c:sub(33)
end
local function secretbox_open(b, n24, k)
	if #b < 16 then return nil end
	local tag, ct = b:sub(1, 16), b:sub(17)
	local ks = xsalsa20_xor(ZERO32, n24, k)
	local want = poly1305(ct, ks)
	local diff = 0
	for i = 1, 16 do diff = diff + (byte(tag, i) == byte(want, i) and 0 or 1) end
	if diff ~= 0 then return nil end
	return xsalsa20_xor(ZERO32 .. ct, n24, k):sub(33)
end
-- nacl box (compatible with Go golang.org/x/crypto/nacl/box Seal/Open)
function M.box_seal(m, n24, pk, sk) return secretbox(m, n24, box_key(pk, sk)) end
function M.box_open(b, n24, pk, sk) return secretbox_open(b, n24, box_key(pk, sk)) end

-- ------------------------------------------------------------------ encodings
local B32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'
local B32R = {}
for i = 1, 32 do B32R[byte(B32, i)] = i - 1 end

function M.base32_encode(s)
	local out, acc, bits = {}, 0, 0
	for i = 1, #s do
		acc = acc * 256 + byte(s, i)
		bits = bits + 8
		while bits >= 5 do
			bits = bits - 5
			local v = floor(acc / 2 ^ bits)
			acc = acc - v * 2 ^ bits
			out[#out + 1] = B32:sub(v + 1, v + 1)
		end
	end
	if bits > 0 then
		local v = acc * 2 ^ (5 - bits)
		out[#out + 1] = B32:sub(v + 1, v + 1)
	end
	return concat(out)
end

function M.base32_decode(s)
	local out, acc, bits = {}, 0, 0
	for i = 1, #s do
		local v = B32R[byte(s, i)]
		if v == nil then
			if byte(s, i) == 61 then break end -- '=' padding
			return nil, 'invalid base32'
		end
		acc = acc * 32 + v
		bits = bits + 5
		if bits >= 8 then
			bits = bits - 8
			local b = floor(acc / 2 ^ bits)
			acc = acc - b * 2 ^ bits
			out[#out + 1] = char(b)
		end
	end
	return concat(out)
end

local B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
function M.base64url(s) -- RFC 4648 url alphabet, no padding (Go base64.RawURLEncoding)
	local out = {}
	for i = 1, #s, 3 do
		local a, b, c = byte(s, i, i + 2)
		local n = a * 65536 + (b or 0) * 256 + (c or 0)
		local k = (c and 4) or (b and 3) or 2
		for j = 1, k do
			local v = floor(n / 2 ^ (18 - 6 * (j - 1))) % 64
			out[#out + 1] = B64:sub(v + 1, v + 1)
		end
	end
	return concat(out)
end

local CRC = {}
for i = 0, 255 do
	local c = i * 256
	for _ = 1, 8 do
		if c >= 32768 then c = bxor((c * 2) % 65536, 0x1021) else c = (c * 2) % 65536 end
	end
	CRC[i] = c
end
function M.crc16(s)
	local crc = 0
	for i = 1, #s do
		local idx = bxor(floor(crc / 256), byte(s, i))
		crc = bxor((crc * 256) % 65536, CRC[idx])
	end
	return crc
end

-- prefix bytes
local P = {
	seed = 18 * 8, private = 15 * 8, server = 13 * 8, cluster = 2 * 8, operator = 14 * 8,
	account = 0, user = 20 * 8, curve = 23 * 8,
}
M.prefix = P
local PNAME = {}
for k, v in pairs(P) do PNAME[v] = k end
local PLETTER = { [P.user] = 'U', [P.account] = 'A', [P.server] = 'N', [P.cluster] = 'C', [P.operator] = 'O',
	[P.curve] = 'X' }

local function with_crc(raw)
	local c = M.crc16(raw)
	return raw .. char(c % 256, floor(c / 256))
end

local function decode_checked(s)
	local raw, err = M.base32_decode(s)
	if not raw then return nil, err end
	if #raw < 4 then return nil, 'invalid nkey' end
	local body, c = raw:sub(1, -3), raw:sub(-2)
	local crc = M.crc16(body)
	if byte(c, 1) ~= crc % 256 or byte(c, 2) ~= floor(crc / 256) then return nil, 'invalid nkey checksum' end
	return body
end

function M.encode_public(prefix, pk) return M.base32_encode(with_crc(char(prefix) .. pk)) end

function M.encode_seed(prefix, seed)
	local b1 = P.seed + floor(prefix / 32)
	local b2 = (prefix % 32) * 8
	return M.base32_encode(with_crc(char(b1, b2) .. seed))
end

-- decode_seed(seed) -> raw 32 byte seed, public prefix byte
function M.decode_seed(s)
	if type(s) ~= 'string' then return nil, 'seed must be a string' end
	s = s:match('^%s*(.-)%s*$')
	local body, err = decode_checked(s)
	if not body then return nil, err end
	local b1, b2 = byte(body, 1, 2)
	if b1 - b1 % 8 ~= P.seed then return nil, 'invalid seed prefix' end
	local prefix = (b1 % 8) * 32 + floor(b2 / 8)
	if not PLETTER[prefix] then return nil, 'invalid public key prefix in seed' end
	local raw = body:sub(3)
	if #raw ~= 32 then return nil, 'invalid seed length' end
	return raw, prefix
end

-- decode_public(key) -> raw 32 byte public key, prefix byte
function M.decode_public(s)
	local body, err = decode_checked(s)
	if not body then return nil, err end
	local prefix = byte(body, 1)
	if not PLETTER[prefix] then return nil, 'invalid public key prefix' end
	return body:sub(2), prefix
end

function M.is_valid_public(s, kind)
	local raw, prefix = M.decode_public(s or '')
	if not raw or #raw ~= 32 then return false end
	return kind == nil or P[kind] == prefix
end

-- ------------------------------------------------------------------ key pairs
local KP = {}
KP.__index = KP

local CKP = {} -- curve (x25519) key pair, ADR / nkeys xkeys
CKP.__index = CKP
M.XKEY_VERSION = 'xkv1'

function M.from_seed(seed)
	local raw, prefix = M.decode_seed(seed)
	if not raw then return nil, prefix end
	local mt = (prefix == P.curve) and CKP or KP
	return setmetatable({ _seed = raw, _prefix = prefix, _encoded = seed:match('^%s*(.-)%s*$') }, mt)
end

local function random_bytes(n)
	local f = io.open('/dev/urandom', 'rb')
	if not f then return nil end
	local r = f:read(n)
	f:close()
	return r
end
M.random_bytes = random_bytes

-- create_pair(kind [, raw_seed]) kind: 'user' | 'account' | 'operator' | 'server' | 'cluster' | 'curve'
-- ('curve' returns an x25519 key pair with seal/open, as nkeys CreateCurveKeys)
-- raw_seed (32 bytes) is required unless /dev/urandom is readable.
function M.create_pair(kind, raw)
	local prefix = P[kind or 'user']
	if not prefix or not PLETTER[prefix] then return nil, 'invalid key type' end
	if not raw then
		raw = random_bytes(32)
		if not raw then return nil, 'no entropy source: pass a 32 byte seed' end
	end
	if #raw ~= 32 then return nil, 'seed must be 32 bytes' end
	return M.from_seed(M.encode_seed(prefix, raw))
end

function KP:seed() return self._encoded end
function KP:type() return PNAME[self._prefix] end
function KP:raw_public()
	if not self._pk then self._pk = ed_public(self._seed) end
	return self._pk
end
function KP:public_key()
	if not self._pub then self._pub = M.encode_public(self._prefix, self:raw_public()) end
	return self._pub
end
function KP:sign(data) return ed_sign(self._seed, data, self:raw_public()) end
function KP:sign_nonce(nonce) return M.base64url(self:sign(nonce)) end

-- curve key pairs: seed/public_key/type like KP, plus seal/open; no signing
function CKP:seed() return self._encoded end
function CKP:type() return 'curve' end
function CKP:raw_public()
	if not self._pk then self._pk = x25519(self._seed, BASE9) end
	return self._pk
end
function CKP:public_key()
	if not self._pub then self._pub = M.encode_public(P.curve, self:raw_public()) end
	return self._pub
end
function CKP:private_key() return M.encode_public(P.private, self._seed) end -- 'P...' (nkeys PrivateKey)
function CKP:sign() return nil, 'invalid curve key operation' end
function CKP:sign_nonce() return nil, 'invalid curve key operation' end

local function curve_public(s)
	local raw, prefix = M.decode_public(s or '')
	if not raw or prefix ~= P.curve or #raw ~= 32 then return nil end
	return raw
end

-- seal(input, recipient_public [, nonce24]) -> 'xkv1' .. nonce .. box   (nkeys Seal)
function CKP:seal(input, recipient, nonce)
	local rpub = curve_public(recipient)
	if not rpub then return nil, 'invalid recipient' end
	nonce = nonce or random_bytes(24)
	if not nonce then return nil, 'no entropy source: pass a 24 byte nonce' end
	if #nonce ~= 24 then return nil, 'nonce must be 24 bytes' end
	return M.XKEY_VERSION .. nonce .. M.box_seal(input, nonce, rpub, self._seed)
end

-- open(sealed, sender_public) -> plaintext | nil, err   (nkeys Open)
function CKP:open(input, sender)
	if type(input) ~= 'string' or #input <= 4 + 24 then return nil, 'invalid encrypted data' end
	if input:sub(1, 4) ~= M.XKEY_VERSION then return nil, 'invalid encryption version' end
	local spub = curve_public(sender)
	if not spub then return nil, 'invalid sender' end
	local plain = M.box_open(input:sub(29), input:sub(5, 28), spub, self._seed)
	if not plain then return nil, 'could not decrypt' end
	return plain
end

-- ------------------------------------------------------------------ creds / JWT (ADR-14)
function M.parse_creds(content)
	local jwt = content:match('%-%-%-%-%-BEGIN NATS USER JWT%-%-%-%-%-%s*([^%s]+)')
	local seed = content:match('%-%-%-%-%-BEGIN USER NKEY SEED%-%-%-%-%-%s*([^%s]+)')
	return jwt, seed
end

function M.format_creds(jwt, seed)
	return concat({
		'-----BEGIN NATS USER JWT-----', jwt, '------END NATS USER JWT------', '',
		'************************* IMPORTANT *************************',
		'NKEY Seed printed below can be used to sign and prove identity.',
		'NKEYs are sensitive and should be treated as secrets.', '',
		'-----BEGIN USER NKEY SEED-----', seed, '------END USER NKEY SEED------', '',
		'*************************************************************', '' }, '\n')
end

-- signer(seed) -> function(nonce) returning the base64url signature (for CONNECT sig)
function M.signer(seed)
	local kp, err = M.from_seed(seed)
	if not kp then return nil, err end
	return function(nonce) return kp:sign_nonce(nonce) end, kp
end

-- issue_user_jwt(account_seed_or_kp, user_public_key, opts) -> jwt (ADR-14)
-- opts: name, expires (unix seconds), issued_at, issuer_account (for scoped signing keys),
--       pub = {allow={},deny={}}, sub = {...}, resp, src, times, locale, tags,
--       subs, data, payload (limits, default -1), bearer_token, allowed_connection_types
function M.issue_user_jwt(account, user_pub, opts)
	opts = opts or {}
	local json = require('nats').json
	local akp = type(account) == 'table' and account or M.from_seed(account)
	if not akp then return nil, 'invalid account seed' end
	if akp:type() ~= 'account' then return nil, 'issuer must be an account key' end
	if not M.is_valid_public(user_pub, 'user') then return nil, 'invalid user public key' end
	local nats_claims = {
		pub = opts.pub or {}, sub = opts.sub or {},
		subs = opts.subs or -1, data = opts.data or -1, payload = opts.payload or -1,
		resp = opts.resp, src = opts.src, times = opts.times, times_location = opts.locale,
		bearer_token = opts.bearer_token, allowed_connection_types = opts.allowed_connection_types,
		issuer_account = opts.issuer_account, tags = opts.tags,
		type = 'user', version = 2,
	}
	local claims = {
		iat = opts.issued_at or os.time(), iss = akp:public_key(), name = opts.name or user_pub,
		sub = user_pub, exp = opts.expires, nats = nats_claims,
	}
	-- jti = base32(sha512_256(claims without jti))
	claims.jti = M.base32_encode(M.sha512_256(json.encode(claims)))
	local header = M.base64url(json.encode({ typ = 'JWT', alg = 'ed25519-nkey' }))
	local payload = header .. '.' .. M.base64url(json.encode(claims))
	return payload .. '.' .. M.base64url(akp:sign(payload))
end

-- decode a JWT without verifying: header, claims
function M.decode_jwt(jwt)
	local json = require('nats').json
	local h, c = jwt:match('^([^.]+)%.([^.]+)%.')
	if not h then return nil, 'invalid jwt' end
	local function b64d(s)
		local rev = {}
		for i = 1, 64 do rev[byte(B64, i)] = i - 1 end
		local out, acc, bits = {}, 0, 0
		for i = 1, #s do
			local v = rev[byte(s, i)]
			if v == nil then break end
			acc, bits = acc * 64 + v, bits + 6
			if bits >= 8 then
				bits = bits - 8
				local b = floor(acc / 2 ^ bits)
				acc = acc - b * 2 ^ bits
				out[#out + 1] = char(b)
			end
		end
		return concat(out)
	end
	local ok1, hd = pcall(json.decode, b64d(h))
	local ok2, cl = pcall(json.decode, b64d(c))
	if not (ok1 and ok2) then return nil, 'invalid jwt json' end
	return hd, cl
end

function M.hex(s) return (s:gsub('.', function(c) return format('%02x', byte(c)) end)) end
function M.unhex(h) return (h:gsub('%x%x', function(x) return char(tonumber(x, 16)) end)) end

return M
