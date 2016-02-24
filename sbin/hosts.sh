# Run a shell command on all hosts.
##

usage="Usage: hosts.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

# If the hosts file is specified in the command line,
# then it takes precedence over the definition in
# succinct-env.sh. Save it here.
if [ -f "$HOSTS_FILE" ]; then
  HOSTLIST=`cat "$HOSTS_FILE"`
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$HOSTS_FILE" = "" ]; then
    if [ -f "hosts" ]; then
      HOSTLIST=`cat hosts`
    else
      HOSTLIST=localhost
    fi
  else
    HOSTLIST=`cat "${HOSTS_FILE}"`
  fi
fi

# By default disable strict host key checking
if [ "$SUCCINCT_SSH_OPTS" = "" ]; then
  SUCCINCT_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for host in `echo "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${SUCCINCT_SSH_FOREGROUND}" ]; then
    ssh $SUCCINCT_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /"
  else
    ssh $SUCCINCT_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /" &
  fi
  if [ "$SUCCINCT_HOST_SLEEP" != "" ]; then
    sleep $SUCCINCT_HOST_SLEEP
  fi
done

wait
