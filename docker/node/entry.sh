#!/bin/bash

PATH="${PATH:-/bin}:/usr/bin"
export PATH

set -euo pipefail
IFS=$'\n\t'

network="$(cat /etc/nano-network)"
case "${network}" in
        live|'')
                network='live'
                dirSuffix=''
                ;;
        beta)
                dirSuffix='Beta'
                ;;
        test)
                dirSuffix='Test'
                ;;
esac

nanodir="${HOME}/RaiBlocks${dirSuffix}"
dbFile="${nanodir}/data.ldb"
mkdir -p "${nanodir}"
if [ ! -f "${nanodir}/config.json" ]; then
        echo "Config File not found, adding default."
        cp "/usr/share/raiblocks/config/${network}.json" "${nanodir}/config.json"
fi

pid=''
firstTimeComplete=''
while true; do
	if [ -n "${firstTimeComplete}" ]; then
		sleep 10
	fi
	firstTimeComplete='true'

	if [ -n "${pid}" ]; then
		if ! kill -0 "${pid}" >/dev/null 2>/dev/null; then
			pid=''
		fi
	fi

	if [ -z "${pid}" ]; then
		/usr/bin/rai_node --daemon &
		pid="$!"
	fi

	if [ -z "${pid}" ]; then
		continue
	fi

	if [ ! -f "${dbFile}" ]; then
		continue
	fi

	dbFileSize="$(stat -c %s "${dbFile}" 2>/dev/null)"
	if [ "${dbFileSize}" -gt $[1024 * 1024 * 1024 * 20] ]; then
		echo "ERROR: Database size grew above 20GB (size = ${dbFileSize})" >&2

		while [ -n "${pid}" ]; do
			kill "${pid}" >/dev/null 2>/dev/null
			if ! kill -0 "${pid}" >/dev/null 2>/dev/null; then
				pid=''
			fi
		done

		/usr/bin/rai_node --vacuum
	fi
done
