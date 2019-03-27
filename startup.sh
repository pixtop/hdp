#!/usr/bin/env bash

tmpdir=${TMPDIR:-/tmp}/pssh.$$
mkdir -p $tmpdir
count=0
if [[ $1 == "start" ]]; then
    while IFS= read -r userhost; do
        ssh -n -o BatchMode=yes ${userhost} 'java -classpath ~/nosave/hidoop/artifacts/hidoop/hidoop.jar ordo.DaemonDataNode' > ${tmpdir}/${userhost} 2>&1 &
        count=`expr $count + 1`
    done < hosts.txt
    while [ $count -gt 0 ]; do
        wait $pids
        count=`expr $count - 1`
    done
elif [[ $1 == "stop" ]]; then
    while IFS= read -r userhost; do
        ssh -n -o BatchMode=yes ${userhost} 'kill $(jps | grep DaemonDataNode | sed 's/DaemonDataNode//')' > ${tmpdir}/${userhost} 2>&1 &
        count=`expr $count + 1`
    done < hosts.txt
    while [ $count -gt 0 ]; do
        wait $pids
        count=`expr $count - 1`
    done
else
  echo "usage: hidoop start|stop"
  exit
fi
echo "Output for hosts are in $tmpdir"
