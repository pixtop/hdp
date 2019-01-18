#!/bin/sh

if test "$#" -ge 1; then
  a=`date +%s%N`
  ${1} >> /dev/null 2>> /dev/null
  b=`date +%s%N`
  diff=$((($b - $a)/1000000))
  echo $diff ' ms'
else echo './perf <cmd>'
fi
