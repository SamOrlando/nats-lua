-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Unit tests for nats-nkeys (SHA-512, ed25519, nkey encoding, JWT) and nats-micro helpers.
-- usage: lua tests/test_nkeys.lua   (run from the repository root)
package.path = "./?.lua;./tests/?.lua;" .. package.path
local nk = require("nats-nkeys")
local micro = require("nats-micro").micro
local H, U = nk.hex, nk.unhex
local pass, fail = 0, 0
local function test(name, fn)
    local ok, err = pcall(fn)
    if ok then pass = pass + 1; print("ok   " .. name) else fail = fail + 1; print("FAIL " .. name .. ": " .. tostring(err)) end
end
local function eq(a, b, m) if a ~= b then error((m or "") .. " expected [" .. tostring(b) .. "] got [" .. tostring(a) .. "]", 2) end end

test("sha512 / sha512_256 of 'abc' and empty", function()
    eq(H(nk.sha512("abc")), "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")
    eq(H(nk.sha512("")), "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e")
    eq(H(nk.sha512_256("abc")), "53048e2681941ef99b2e29b76b4c7dabe4c2d0c634fc6d46e0e2f13107e7af23")
    eq(#nk.sha512(string.rep("a", 1000)), 64)
end)

local rfc = {
    { "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60", "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a", "",
      "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b" },
    { "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb", "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c", "72",
      "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00" },
    { "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7", "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025", "af82",
      "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a" },
}
test("ed25519 RFC 8032 vectors 1-3", function()
    for i, v in ipairs(rfc) do
        local pk = nk.ed25519_public(U(v[1]))
        eq(H(pk), v[2], "pub " .. i)
        eq(H(nk.ed25519_sign(U(v[1]), U(v[3]), pk)), v[4], "sig " .. i)
    end
end)

test("nkeys Go vector (user seed, public key, signature)", function()
    local kp = assert(nk.from_seed("SUANU6Z2W3GZXAGW7HW542S7DV64FPWMCN3DXKNMJU5DLTONOQDP4UFKNY"))
    eq(kp:public_key(), "UDHCE3WRB3YPXVFQR34XLEURROUWO22W377GUOHCY7SCO2DBLWH7QSJT")
    eq(nk.base64url(kp:sign("hello")), "gYx3rxjToSV331HzOj58Pz0uRg17zMqQX3Qt2lZGUlOGOwLYdjUr55Ej2hhu63zOVbViud9WipQgZwf1LGSLCg")
    assert(nk.is_valid_public(kp:public_key()))
    assert(not nk.is_valid_public("UDHCE3WRB3YPXVFQR34XLEURROUWO22W377GUOHCY7SCO2DBLWH7QSJA"), "bad crc rejected")
end)

test("base32 / crc16 round trip", function()
    eq(nk.crc16("123456789"), 0x31C3) -- CRC-16/XMODEM check value
    for _, s in ipairs({ "", "f", "fo", "foo", "foob", "fooba", "foobar" }) do eq(nk.base32_decode(nk.base32_encode(s)), s) end
    eq(nk.base32_encode("foobar"), "MZXW6YTBOI")
    local kp = nk.create_pair("account")
    eq(kp:public_key():sub(1, 1), "A"); eq(kp:seed():sub(1, 2), "SA")
    eq(nk.from_seed(kp:seed()):public_key(), kp:public_key())
end)

test("issue and decode user JWT, creds round trip", function()
    local acct, user = nk.create_pair("account"), nk.create_pair("user")
    local jwt = assert(nk.issue_user_jwt(acct, user:public_key(), { name = "t" }))
    local hdr, claims = nk.decode_jwt(jwt)
    eq(hdr.alg, "ed25519-nkey"); eq(claims.sub, user:public_key()); eq(claims.iss, acct:public_key())
    local j2, s2 = nk.parse_creds(nk.format_creds(jwt, user:seed()))
    eq(j2, jwt); eq(s2, user:seed())
end)

test("x25519 RFC 7748 6.1 and poly1305 RFC 8439 2.5.2", function()
    local a = U("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
    local b = U("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
    eq(H(nk.x25519_base(a)), "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
    eq(H(nk.x25519_base(b)), "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f")
    eq(H(nk.x25519(a, nk.x25519_base(b))), "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")
    eq(H(nk.poly1305("Cryptographic Forum Research Group",
        U("85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b"))), "a8061dc1305136c6c22b8baf0c0127a9")
end)

test("xkeys: curve pair, Go nkeys vector, seal/open", function()
    -- vector from tests/interop/nkeys (nkeyscheck xvector): seeds 32x01 / 32x02, nonce 24x07
    local a = assert(nk.from_seed("SXAACAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAN4QM"))
    local b = assert(nk.from_seed("SXAAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAQCAIBAEAT5QQ"))
    eq(a:type(), "curve")
    eq(a:public_key(), "XCSOBEUSWZI4E6FZO4WFNH27VG5RHWIGWRVLNDE57HOCWRAJ7CRATSUK")
    eq(b:public_key(), "XDHI2OWRZS3DH3D3ODAXQFFFY5XM2AUWQUCQ2NCHIW5ALBYOLB6VSWAL")
    local sealed = assert(a:seal("hello xkeys", b:public_key(), string.rep("\7", 24)))
    eq(H(sealed), "786b76310707070707070707070707070707070707070707070707073b4845e98228ced59bf6ae706ae523a540e1ba079404c1719cd5e0")
    eq(b:open(sealed, a:public_key()), "hello xkeys")
    local _, e = b:open(sealed:sub(1, -2) .. "x", a:public_key()); eq(e, "could not decrypt")
    _, e = b:open("xkv2" .. sealed:sub(5), a:public_key()); eq(e, "invalid encryption version")
    _, e = a:seal("x", "UDHCE3WRB3YPXVFQR34XLEURROUWO22W377GUOHCY7SCO2DBLWH7QSJT"); eq(e, "invalid recipient")
    eq(select(2, a:sign("x")), "invalid curve key operation")
    local c = assert(nk.create_pair("curve"))
    eq(c:public_key():sub(1, 1), "X"); eq(c:seed():sub(1, 2), "SX"); eq(c:private_key():sub(1, 1), "P")
    local long = string.rep("0123456789abcdef", 20)
    eq(c:open(a:seal(long, c:public_key()), a:public_key()), long)
    assert(nk.is_valid_public(c:public_key(), "curve"))
end)

test("micro semver validation", function()
    for _, v in ipairs({ "0.0.1", "1.2.3-alpha.1+build.5", "10.20.30" }) do assert(micro.valid_version(v), v) end
    for _, v in ipairs({ "1.2", "01.2.3", "1.2.3-", "v1.2.3", "" }) do assert(not micro.valid_version(v), v) end
end)

print(string.format("%s nkeys: %d passed, %d failed", _VERSION, pass, fail))
os.exit(fail == 0 and 0 or 1)
