#!/bin/bash

set -e

RUNS=25 # 25*4 = 100
OPPS_PERCENT=25
echo "STARTING wc_40G_80p"
/experiment/reset.sh
for i in $(seq 1 $RUNS)
do
	declare -a pids
	echo "--------------------[$i]-----------------------------"
	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/40G-file.txt /wordcount/output/wc_$(date +%s) &
	pids+=($!)
	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/40G-file.txt /wordcount/output/wc_$(date +%s) &
	pids+=($!)
	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/40G-file.txt /wordcount/output/wc_$(date +%s) &
	pids+=($!)
	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/40G-file.txt /wordcount/output/wc_$(date +%s) &
	pids+=($!)
	wait ${pids[@]}
	pids=""
done
mkdir -pv /experiment/wc/40G/80p
pushd /experiment/wc/40G/80p
python3 /experiment/get_experiment_data.py &> /dev/null
hadoop fs -get /tmp ./staging
popd
tar -czf wc_40G_80p.tar.gz /experiment/wc/40G/80p
mv wc_40G_80p.tar.gz /experiment/archive
echo "FINISH wc_40G_80p"

#echo "STARTING wc_20G_80p"
#/experiment/reset.sh
#for i in $(seq 1 $RUNS)
#do
#	declare -a pids
#	echo "--------------------[$i]-----------------------------"
#	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/20G-file.txt /wordcount/output/wc_$(date +%s) &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/20G-file.txt /wordcount/output/wc_$(date +%s) &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/20G-file.txt /wordcount/output/wc_$(date +%s) &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES wordcount /wordcount/input/20G-file.txt /wordcount/output/wc_$(date +%s) &
#	pids+=($!)
#	wait ${pids[@]}
#	pids=""
#done
#mkdir -pv /experiment/wc/20G/80p
#pushd /experiment/wc/20G/80p
#python3 /experiment/get_experiment_data.py &> /dev/null
#hadoop fs -get /tmp ./staging
#popd
#tar -czf wc_20G_80p.tar.gz /experiment/wc/20G/80p
#mv wc_20G_80p.tar.gz /experiment/archive
#echo "FINISH wc_20G_80p"
