<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-nkeys` — NKEYs, ed25519, creds, user JWTs

`local nkeys = require('nats-nkeys')` returns its own table (not `nats`). Pure
Lua SHA-512 and ed25519 (TweetNaCl port) for Lua 5.1–5.4 and LuaJIT; uses
`luasodium` or `luatweetnacl` for ed25519 when installed (`nkeys.backend`).
The core client uses it automatically for `creds`, `nkey_seed`, `nkey_file`.
ADR-14. Strings are raw bytes unless noted. Pure Lua signing takes ~0.2–0.4 s
per signature on PUC Lua (once per connect).

## Key pairs

### nkeys.from_seed(seed)

Returns: [kp](#object-key-pair-kp) for an encoded seed (`SU...`, `SA...`, ...;
surrounding whitespace ignored), or `nil, err` (`'seed must be a string'`,
`'invalid base32'`, `'invalid nkey checksum'`, `'invalid seed prefix'`, ...).

```lua
local kp = assert(nkeys.from_seed(io.open('user.nk'):read('*l')))
print(kp:public_key())
```

### nkeys.create_pair(kind, raw)

`kind`: `'user'` (default), `'account'`, `'operator'`, `'server'`, `'cluster'`
(also `'curve'`, see API notes). `raw`: 32 random bytes, read from
`/dev/urandom` when omitted. Returns: kp or `nil, 'invalid key type' | 'no
entropy source: pass a 32 byte seed' | 'seed must be 32 bytes'`.

```lua
local acct = assert(nkeys.create_pair('account'))
```

### Object: key pair (`kp`)

#### kp:seed()

Returns: the encoded seed string.

#### kp:public_key()

Returns: the encoded public key (`U...`, `A...`, `O...`, `N...`, `C...`).

#### kp:raw_public()

Returns: 32 byte raw public key.

#### kp:type()

Returns: `'user' | 'account' | 'operator' | 'server' | 'cluster' | 'curve'`.

#### kp:sign(data)

Returns: 64 byte raw ed25519 signature.

#### kp:sign_nonce(nonce)

Returns: base64url (no padding) signature, as sent in CONNECT `sig`.

## Creds and JWT

### nkeys.signer(seed)

Returns: `function(nonce) → sig, kp`, or `nil, err`. Suitable as a
`signature_cb`.

### nkeys.parse_creds(content)

Returns: `jwt, seed` (either may be nil) from creds text.

### nkeys.format_creds(jwt, seed)

Returns: creds file text in the nsc format.

### nkeys.issue_user_jwt(account, user_pub, opts)

Issues a user JWT signed by an account key (ADR-14). `account`: account seed or
kp. `user_pub`: user public key.

| `opts` field | Type | Default |
|---|---|---|
| `name` | string | `user_pub` |
| `expires`, `issued_at` | unix seconds | –, now |
| `issuer_account` | string | – (signing keys) |
| `pub`, `sub` | `{allow = {...}, deny = {...}}` | `{}` |
| `resp`, `src`, `times`, `locale`, `tags`, `allowed_connection_types`, `bearer_token` | | – |
| `subs`, `data`, `payload` | number | -1 |

Returns: JWT string, or `nil, 'invalid account seed' | 'issuer must be an
account key' | 'invalid user public key'`.

```lua
local acct, user = nkeys.create_pair('account'), nkeys.create_pair('user')
local jwt = assert(nkeys.issue_user_jwt(acct, user:public_key(), { name = 'svc' }))
io.open('svc.creds', 'w'):write(nkeys.format_creds(jwt, user:seed()))
```

### nkeys.decode_jwt(jwt)

Decodes without verifying. Returns: `header, claims` or `nil, 'invalid jwt' |
'invalid jwt json'`.

## Encoding

### nkeys.encode_public(prefix, raw_pk) / nkeys.encode_seed(prefix, raw_seed)

`prefix`: a byte from `nkeys.prefix` (`user = 160`, `account = 0`,
`operator = 112`, `server = 104`, `cluster = 16`, `curve = 184`, `seed = 144`).
Returns: encoded key.

### nkeys.decode_public(s) / nkeys.decode_seed(s)

Returns: `raw (32 bytes), prefix` or `nil, err`.

### nkeys.is_valid_public(s, kind)

Returns: bool (`kind` optional, e.g. `'user'`).

### nkeys.base32_encode(s) / nkeys.base32_decode(s)

RFC 4648 base32 without padding (decode stops at `=`). `decode` returns
`nil, 'invalid base32'` on bad input.

### nkeys.base64url(s)

Returns: base64url without padding (Go `RawURLEncoding`).

### nkeys.crc16(s)

Returns: CRC-16/XMODEM as a number.

### nkeys.hex(s) / nkeys.unhex(h)

Hex-encode raw bytes / decode hex.

## Hashing and signing primitives

### nkeys.sha512(s) / nkeys.sha512_256(s)

Returns: 64 / 32 byte digest.

```lua
print(nkeys.hex(nkeys.sha512_256('abc')))
```

### nkeys.ed25519_public(seed32) / nkeys.ed25519_sign(seed32, msg, pk)

Active backend. `pk` (optional) avoids recomputing the public key (pure Lua
backend only). Returns: 32 byte public key / 64 byte signature.

### nkeys.ed25519_public_lua(seed32) / nkeys.ed25519_sign_lua(seed32, msg, pk)

Always the pure Lua implementation.

Fields: `nkeys.backend` (`'lua' | 'luasodium' | 'luatweetnacl'`), `nkeys.prefix`.
