set -e

	#-Dgridmix.sleep.max-map-time=29000 \
run-gridmix () {
	echo "RUNNING DIST-MAPS=$1%-MAX-MAP=$2"
	OUTDIR=/experiment/distributed/50-NODES/$2/MAPS-70/DIST-$1
	source /experiment/env.sh
	/experiment/reset.sh
	yarn --config /yarn-conf/hadoop jar $GRIDMIX \
		-Dgridmix.job.type=SLEEPJOB \
		-Dgridmix.slepp.maptask-only=true \
		-Dmapreduce.job.num-opportunistic-maps-percent=$1 \
		/gridmix file:///experiment/rumen/short-pi-100j-70t-60n-trace.json
		#/gridmix file:///experiment/rumen/pi_j100_t50_16000_trace.json
		#/gridmix file:///experiment/rumen/200j-200t-short-trace.json  
		#/gridmix file:///experiment/gridmix/rumen/long-153t-trace.json
		#-Dgridmix.sleep.max-map-time=$2 \
	mkdir -pv $OUTDIR
	pushd $OUTDIR
	python3 /experiment/get_experiment_data.py &> /dev/null
	hadoop fs -get /tmp ./staging
	popd
	#tar -cvzf DIST-$1.tar.gz $OUTDIR
	#mv DIST-$1%-$2.tar.gz /experiment/archive
	echo "FINISH DIST-MAPS=$1%-MAX-MAP=$2"
}


# 1: opportunistic percentage
# 2: Type
# 3: trace_path
# 4: max_sleep
run-experiment () {
	OPPS=$1
	TYPE=$2
	TPATH=$3
	MAX_SLEEP=$4
	NODES=$5
	echo "RUNNING DIST-MAPS=$1% Type=$2 path=$3 max-sleep"
	OUTDIR=/experiment/distributed/NODE-$NODES/$TYPE/DIST-$OPPS
	source /experiment/env.sh
	/experiment/reset.sh
	yarn --config /yarn-conf/hadoop jar $GRIDMIX \
		-Dgridmix.job.type=SLEEPJOB \
		-Dgridmix.slepp.maptask-only=true \
		-Dmapreduce.job.num-opportunistic-maps-percent=$OPPS \
		-Dgridmix.sleep.max-map-time=$MAX_SLEEP \
		/gridmix file://$TPATH
	mkdir -pv $OUTDIR
	pushd $OUTDIR
	python3 /experiment/get_experiment_data.py &> /dev/null
	hadoop fs -get /tmp ./staging
	popd
	echo "FINISH DIST-MAPS=$1%-MAX-MAP=$2"
}

# 1: number of nodes
create_samples() {
	NODES=$1
	#start yarn
	stop-yarn.sh
	cp -fv /worker-files/workers-$NODES /opt/hadoop/etc/hadoop/workers
	start-yarn.sh
	# 20 second max sleep
	run-experiment 0   "MIXED" "/experiment/rumen/mix-pi-trace.json" 20000 $NODES 
	run-experiment 25  "MIXED" "/experiment/rumen/mix-pi-trace.json" 20000 $NODES 
	run-experiment 50  "MIXED" "/experiment/rumen/mix-pi-trace.json" 20000 $NODES 
	run-experiment 75  "MIXED" "/experiment/rumen/mix-pi-trace.json" 20000 $NODES 
	run-experiment 100 "MIXED" "/experiment/rumen/mix-pi-trace.json" 20000 $NODES 
	#short
	run-experiment 0   "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 25  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 50  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 75  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 100 "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	#long
	run-experiment 0   "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 25  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 50  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 75  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 100 "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	#start yarn
# 1: number of nodes
}
pad_samples() {
	NODES=$1
	#start yarn
	stop-yarn.sh
	cp -fv /worker-files/workers-$NODES /opt/hadoop/etc/hadoop/workers
	start-yarn.sh
	# 20 second max sleep
	run-experiment 25  "MIXED" "/experiment/rumen/mixed-pi-trace.json" 20000 $NODES 
	run-experiment 50  "MIXED" "/experiment/rumen/mixed-pi-trace.json" 20000 $NODES 
	run-experiment 75  "MIXED" "/experiment/rumen/mixed-pi-trace.json" 20000 $NODES 
	#short
	run-experiment 25  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 50  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	run-experiment 75  "SHORT" "/experiment/rumen/short-pi-100j-70t-60n-trace.json" 1500 $NODES 
	#long
	run-experiment 25  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 50  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	run-experiment 75  "LONG" "/experiment/rumen/long-pi-100j-70t-60n-trace.json" 15000 $NODES 
	#start yarn
}
pad_samples 60
pad_samples 50
pad_samples 40
pad_samples 30
pad_samples 20
pad_samples 10
