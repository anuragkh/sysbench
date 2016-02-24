#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

if [ "$SUCCINCT_RES_PATH" = "" ]; then
	SUCCINCT_RES_PATH="res"
fi

mkdir -p $SUCCINCT_RES_PATH

"$sbin/hosts.sh" cd "$sbin" \; "./start-thputbench-local.sh" "$@"
"$sbin/hosts.sh" cd "$sbin" \; awk '{ sum += \$1 } END { print sum }' thput > "$SUCCINCT_RES_PATH/thput"
"$sbin/hosts.sh" cd "$sbin" \; rm thput
