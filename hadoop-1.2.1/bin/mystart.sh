#!/usr/bin/env bash

# File created by Jakob Runge (2013-12-02)
# as a way to start the hadoop setup easily.

datanodeLog=../logs/hadoop-mystart-datanode.log
tasktrackerLog=../logs/hadoop-mystart-tasktracker.log

./start-dfs.sh
./hadoop --config ../conf datanode 2> $datanodeLog&
./start-mapred.sh
./hadoop --config ../conf tasktracker 2> $tasktrackerLog&
