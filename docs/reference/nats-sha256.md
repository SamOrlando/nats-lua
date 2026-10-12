<!-- Copyright (c) 2023-2026 Sam Orlando. SPDX-License-Identifier: MIT -->
# `nats-sha256` — SHA-256

`local sha256 = require('nats-sha256')` returns its own table. Uses luaossl
(`openssl.digest`) when installed, otherwise pure Lua (native integer ops on
5.3+, `bit` on LuaJIT, `bit32` on 5.2, lookup tables on 5.1). Used by the object
store digests.

### sha256.new()

Returns: a [hasher](#object-hasher) using the active backend.

```lua
local h = sha256.new(); h:update('ab'):update('c'); local raw = h:final()
```

### sha256.new_lua()

Returns: a pure Lua hasher regardless of backend.

### sha256.digest(s)

Returns: 32 byte raw digest of `s`.

### sha256.hex(s)

Returns: lowercase hex **digest** of `s` (it hashes; compare `nkeys.hex`, which
only hex-encodes).

```lua
assert(sha256.hex('abc') == 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
```

## Object: hasher

### hasher:update(s)

Adds data (`tostring`ed). Returns: the hasher (chainable).

### hasher:final()

Returns: 32 byte raw digest. Do not reuse the hasher afterwards.

Fields: `sha256.backend` (`'luaossl' | 'lua-int' | 'luajit-bit' | 'lua-bit32' |
'lua-tables'`). `_band`, `_bxor`, `_impl` are internal.
