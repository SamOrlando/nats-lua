#!/bin/sh
# Run unit tests (and integration tests when NATS_URL is set) on every
# available Lua interpreter. lua-nuid must be on LUA_PATH (or set NUID_DIR).
cd "$(dirname "$0")/.." || exit 1
LUA_PATH="./?.lua;./tests/?.lua;${NUID_DIR:+$NUID_DIR/?.lua;};"
export LUA_PATH
rc=0
for lua in lua5.1 lua5.2 lua5.3 lua5.4 luajit; do
    command -v "$lua" >/dev/null 2>&1 || continue
    "$lua" tests/test_nats.lua || rc=1
    "$lua" tests/test_objs.lua || rc=1
    if [ -n "$NATS_URL" ]; then "$lua" tests/test_integration.lua | tail -1 || rc=1; fi
done
exit $rc
