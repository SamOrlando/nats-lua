-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- helper for tests/interop/xkeys_interop.sh: new | seal <seed> <recipient> <hex> | open <seed> <sender> <hex>
package.path="./?.lua;"..package.path
local nk=require"nats-nkeys"
local mode=arg[1]
if mode=='new' then local k=nk.create_pair("curve") print(k:seed().." "..k:public_key())
elseif mode=='open' then local k=nk.from_seed(arg[2]) print(nk.hex(assert(k:open(nk.unhex(arg[4]),arg[3]))))
else local k=nk.from_seed(arg[2]) print(nk.hex(assert(k:seal(nk.unhex(arg[4]),arg[3])))) end
