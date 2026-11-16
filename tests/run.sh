#!/bin/sh
# Run tests on every available Lua interpreter. lua-nuid must be on LUA_PATH
# (or set NUID_DIR). test_nats.lua needs a NATS server on 127.0.0.1:4222 and
# nats-server on PATH (or NATS_SERVER_BIN) for auth and lame duck.
# test_functions.lua starts its own JetStream nats-server on 127.0.0.1:14422
# (monitor 18422, websocket 18480, TLS 14423).
# Integration tests run when NATS_URL is set. test_local runs when
# NATS_SERVER_BIN is set. Exits non-zero when any suite fails.
cd "$(dirname "$0")/.." || exit 1
LUA_PATH="./?.lua;./tests/?.lua;${NUID_DIR:+$NUID_DIR/?.lua;};"
export LUA_PATH
rc=0
failed=""
run() { # run <lua> <suite>
    if ! "$1" "tests/$2.lua"; then rc=1; failed="$failed $1:$2"; fi
}
for lua in ${LUAS:-lua5.1 lua5.2 lua5.3 lua5.4 luajit}; do
    command -v "$lua" >/dev/null 2>&1 || continue
    run "$lua" test_nats
    run "$lua" test_functions
    run "$lua" test_objs
    run "$lua" test_nkeys
    run "$lua" check_docs
    if [ -n "$NATS_SERVER_BIN" ]; then run "$lua" test_local; fi
    if [ -n "$NATS_URL" ]; then run "$lua" test_integration; fi
done
if [ $rc -ne 0 ]; then echo "FAILED:$failed"; else echo "all suites passed"; fi
exit $rc
