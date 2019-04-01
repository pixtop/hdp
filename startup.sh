#!/usr/bin/env bash

tmpdir=${TMPDIR:-/tmp}/hidoop/pssh.$$
shpath=~/nosave/hidoop
mkdir -p $tmpdir
count=0
if [[ $1 == "start" ]]; then
    while IFS= read -r userhost; do
        ssh -n -o BatchMode=yes ${userhost} "$shpath/hidoop.sh start $2" > ${tmpdir}/${userhost} 2>&1 &
        count=`expr $count + 1`
    done < hosts.txt
    while [ $count -gt 0 ]; do
        wait $pids
        count=`expr $count - 1`
    done
elif [[ $1 == "stop" ]]; then
    while IFS= read -r userhost; do
        ssh -n -o BatchMode=yes ${userhost} "$shpath/hidoop.sh stop" > ${tmpdir}/${userhost} 2>&1 &
        count=`expr $count + 1`
    done < hosts.txt
    while [ $count -gt 0 ]; do
        wait $pids
        count=`expr $count - 1`
    done
else
  echo "usage: startup start|stop [dataNode] [daemonMonitor]"
  exit
fi
echo "Output for hosts are in $tmpdir"
