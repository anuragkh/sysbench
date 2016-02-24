# Usage: start-thputbench-local.sh [-n <#threads>] [-t <bench-type>] [-a <append-file>] [-q <query-file>]

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

python "$sbin/../elasticsearch/perf/esthroughput.py" "$@" 2>&1 &
