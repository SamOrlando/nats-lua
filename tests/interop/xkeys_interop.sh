#!/bin/sh
# Copyright (c) 2023-2026 Sam Orlando
# SPDX-License-Identifier: MIT (see LICENSE)
# Go nkeys <-> pure Lua xkeys interop. Usage: NKEYSCHECK_BIN=/path/nkeyscheck tests/interop/xkeys_interop.sh [N]
# Build nkeyscheck: (cd tests/interop/nkeys && go build -o /tmp/nkeyscheck .)
set -e
NK=${NKEYSCHECK_BIN:-/tmp/interop/nkeyscheck}
N=${1:-10}; H=tests/interop/xkeys_helper.lua; fail=0
for L in ${LUAS:-lua5.1 lua5.2 lua5.3 lua5.4 luajit}; do
 command -v $L >/dev/null || continue
 ok=0; i=0
 while [ $i -lt $N ]; do i=$((i+1))
  R=$($L $H new); RS=${R% *}; RP=${R#* }
  PLAIN=$(head -c $((1 + i * 37)) /dev/urandom | od -An -tx1 -v | tr -d ' \n')
  OUT=$($NK xseal $RP $PLAIN); SP=$(echo "$OUT"|awk '/^PUB/{print $2}'); SEALED=$(echo "$OUT"|awk '/^SEALED/{print $2}')
  [ "$($L $H open $RS $SP $SEALED)" = "$PLAIN" ] || { echo "$L go->lua FAIL"; fail=1; }
  G=$($NK xgen); GS=$(echo "$G"|awk '/^SEED/{print $2}'); GP=$(echo "$G"|awk '/^PUB/{print $2}')
  S2=$($L $H seal $RS $GP $PLAIN)
  [ "$($NK xopen $GS $RP $S2 | awk '{print $2}')" = "$PLAIN" ] || { echo "$L lua->go FAIL"; fail=1; }
  ok=$((ok+1))
 done
 echo "$L xkeys interop: $ok rounds (go->lua, lua->go)"
done
exit $fail
