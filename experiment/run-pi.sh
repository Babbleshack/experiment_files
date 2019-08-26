set -e
RUNS=100
CHUNK=20
N_MAPS=70
N_SAMPLES=10000000 # 10^8
OPP_PERCENT=0

YARN="yarn --config /yarn-conf/hadoop" 
YARN_CONF=/yarn-conf/hadoop
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
	for i in $(seq 1 $CHUNK $RUNS);
	do
		echo $i
		declare -a pids
		for y in $(seq 1 $CHUNK);
		do
			yarn --config $YARN_CONF jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent=0 $MAPS $SAMPLES & 
		done
		wait
	done
	mkdir -pv $OUTDIR
	pushd $OUTDIR
	python3 /experiment/get_experiment_data.py &> /dev/null
	hadoop fs -get /tmp ./staging
	popd
	echo "FINNISHED PI MAPS=$MAPS SAMPLES=$SAMPLES TYPE=$TYPE"
}

run-mixed-pi () {
	echo "STARTING MIXED RUN op=$1 maps=$2"
	source /experiment/env.sh
	/experiment/reset.sh
	OUTDIR="/experiment/distributed/golden/MIXED/"
	for i in $(seq 1 $CHUNK $RUNS);
	do
		long=TRUE # flag used for switching 
		declare -a pids
		for y in $(seq 1 $CHUNK);
		do
			if [ $long ]
			then
				echo "RUNNING LONG PI"
				yarn --config $YARN_CONF jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent=$1 $2 1000000000 & 
				long=
			else
				echo "RUNNING SHORT PI"
				yarn --config $YARN_CONF jar $MAPRED_EXAMPLES pi -Dmapreduce.job.num-opportunistic-maps-percent=$1 $2 1000000 & 
				long=true
			fi
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

#create_trace $N_MAPS 1000000000 "LONG"
#create_trace $N_MAPS 1000000 "SHORT"
run-mixed-pi 0 $N_MAPS
