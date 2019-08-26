#!/bin/bash
#hadoop fs -rm -r /gridmix/input
hadoop fs -rm -r /gridmix/gridmix
hadoop fs -rm -r /tmp
#mr-jobhistory-daemon.sh stop historyserver
#mr-jobhistory-daemon.sh start historyserver
mapred --daemon stop historyserver
mapred --daemon start historyserver
