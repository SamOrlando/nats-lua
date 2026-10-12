rockspec_format = "3.0"
package = "nats-lua"
version = "0.1.0-1"
source = {
   url = "git+https://git.nodex.net:9002/odi/nats-lua.git",
   tag = "v0.1.0",
}
description = {
   summary = "NATS client for Lua with headers, request/reply, reconnect, JetStream and KV",
   detailed = [[
Pure Lua NATS client on LuaSocket: pub/sub, queue groups, headers
(HPUB/HMSG), muxed request/reply with no-responders detection, PING/PONG
keepalive, reconnect with server pool and backoff, drain, max_payload and
subject validation, optional TLS (LuaSec), JetStream streams, pull
consumers and Key-Value buckets. Lua 5.1-5.4 and LuaJIT.
]],
   homepage = "https://git.nodex.net:9002/odi/nats-lua",
   license = "MIT",
   labels = { "nats", "messaging", "jetstream", "pubsub" },
}
dependencies = {
   "lua >= 5.1, < 5.5",
   "luasocket >= 3.0",
   "nuid >= 1.0.0",
}
-- optional at runtime: lua-cjson (faster JSON, pure Lua fallback built in),
-- luasec (TLS)
build = {
   type = "builtin",
   modules = {
      ["nats"] = "nats.lua",
      ["nats-jetstream"] = "nats-jetstream.lua",
      ["nats-kvs"] = "nats-kvs.lua",
      ["nats-objs"] = "nats-objs.lua",
   },
}
test = {
   type = "command",
   script = "tests/test_nats.lua",
}
