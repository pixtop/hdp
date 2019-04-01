#!/usr/bin/env bash

tmpdir=/tmp/hidoop_pvd
jarpath=~/nosave/hidoop

mkdir -p $tmpdir

if [[ $1 == "start" ]]; then
  java -jar $jarpath/HdfsServer.jar $tmpdir -h $2&
  java -jar $jarpath/HidoopServer.jar $tmpdir
elif [[ $1 == "stop" ]]; then
  kill $(jps | grep jar | sed 's/jar//')
else
  echo "usage: hidoop start|stop [dataNode]"
  exit
fi
