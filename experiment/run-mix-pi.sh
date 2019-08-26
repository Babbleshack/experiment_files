set -e
run-pi () {
	echo "STARTING mixed-pi op=$1 maps=$2"
	source /experiment/env.sh
	/experiment/reset.sh
	OUTDIR="/experiment/distributed/golden/MIXED/"
	for i in $(seq 1 $CHUNK $N_RUNS);
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

RUNS=100
CHUNK=10
N_MAPS=70
N_SAMPLES=10000000 # 10^8
OPP_PERCENT=0

YARN="yarn --config /yarn-conf/hadoop" 
YARN_CONF=/yarn-conf/hadoop

run-pi 0 $N_MAPS $N_SAMPLES
