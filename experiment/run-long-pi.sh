set -e
run-pi () {
	echo "STARTING mixed-pi op=$1 maps=$2"
	source /experiment/env.sh
	/experiment/reset.sh
	OUTDIR="/experiment/distributed/golden/long-pi/"
	for i in $(seq 1 $CHUNK $N_RUNS);
	do
		long=TRUE # flag used for switching 
		declare -a pids
		for y in $(seq 1 $CHUNK);
		do
			echo "RUNNING LONG PI"
			yarn --config $YARN_CONF jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent=$1 $2 1000000000 & 
			long=
		done
		wait
		pids=""
	done
	mkdir -pv $OUTDIR
	pushd $OUTDIR
	python3 /experiment/get_experiment_data.py &> /dev/null
	hadoop fs -get /tmp ./staging
	popd
	echo "FINNISHED mixed-pi op=$1 maps=$2"
}

# 1: MAPS
# 2: SAMPLES
# 3: TYPE
create_trace () {
	MAPS=$1
	SAMPLES=$2
	TYPE=$3
	echo "STARTING PI MAPS=$MAPS SAMPLES=$SAMPLES TYPE=$TYPE"
	source /experiment/env.sh
	/experiment/reset.sh
	OUTDIR="/experiment/distributed/golden/$TYPE/"
	for i in $(seq 1 $CHUNK $N_RUNS);
	do
		declare -a pids
		for y in $(seq 1 $CHUNK);
		do
			yarn --config $YARN_CONF jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent=0 $MAPS $TYPE & 
			long=
		done
		wait
		pids=""
	done
	mkdir -pv $OUTDIR
	pushd $OUTDIR
	python3 /experiment/get_experiment_data.py &> /dev/null
	hadoop fs -get /tmp ./staging
	popd
	echo "FINNISHED PI MAPS=$MAPS SAMPLES=$SAMPLES TYPE=$TYPE"
}

RUNS=100
CHUNK=10
N_MAPS=70
N_SAMPLES=10000000 # 10^8
OPP_PERCENT=0

YARN="yarn --config /yarn-conf/hadoop" 
YARN_CONF=/yarn-conf/hadoop


run-pi 0 $N_MAPS $N_SAMPLES

create_trace $N_MAPS 1000000000 "LONG"
create_trace $N_MAPS 1000000 "SHORT"


#for i in {1..50}
#do
#	declare -a pids
#	echo "--------------------[$i]-----------------------------"
#	nohup yarn jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent="$OPP_PERCENT" $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent="$OPP_PERCENT" $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent="$OPP_PERCENT" $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup yarn jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent="$OPP_PERCENT" $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	wait ${pids[@]}
#	pids=""
#done
#for i in {1..50}
#do
#	declare -a pids
#	echo "--------------------[$i]-----------------------------"
#	nohup $YARN jar $MAPRED_EXAMPLES pi  $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup $YARN jar $MAPRED_EXAMPLES pi  $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup $YARN jar $MAPRED_EXAMPLES pi  $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	nohup $YARN jar $MAPRED_EXAMPLES pi  $N_MAPS $N_SAMPLES &
#	pids+=($!)
#	wait ${pids[@]}
#	pids=""
#done
#mkdir -pv /experiment/distributed/benchmark-quasipi-200
#pushd /experiment/distributed/benchmark-quasipi-200
#python3 /experiment/get_experiment_data.py
#hadoop fs -get /tmp ./staging
#/experiment/reset.sh
