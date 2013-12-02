#!/usr/bin/env bash

# File created by Jakob Runge (2013-12-02)
# as a way to stop the hadoop setup easily.

./stop-dfs.sh
./stop-mapred.sh
# Stopping TaskTracker and Datanode:
toStop=`jps | grep -P '.*(DataNode|TaskTracker)$'`
isNumb='^[0-9]+$'
for stopMe in $toStop; do
  if [[ $stopMe =~ $isNumb ]] ; then
    kill $stopMe
  fi
done
