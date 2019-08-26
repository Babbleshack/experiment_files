set -e
W_PID=13084
echo "Started waiting on $W_PID"
while [ -e /proc/$W_PID ]
do
    echo "Process: $W_PID is still running"
    sleep 10
done
echo "STARTING NO SLEEP GRIDMIX"
	#-Dgridmix.sleep.max-map-time=29000 \
run-gridmix () {
	echo "RUNNING DIST-MAPS=$1%-MAX-MAP=$2"
	OUTDIR=/experiment/distributed/NO_SLEEP/$2/DIST-$1
	source /experiment/env.sh
	/experiment/reset.sh
	yarn --config /yarn-conf/hadoop jar $GRIDMIX \
		-Dmapreduce.job.num-opportunistic-maps-percent=$1 \
		/gridmix file:///experiment/rumen/short-pi-100j-50t-trace.json
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

#run-gridmix 0
#run-gridmix 100
#run-gridmix 50
#run-gridmix 25 1500
run-gridmix 0 pi
run-gridmix 25 pi
run-gridmix 50 pi
run-gridmix 75 pi
run-gridmix 100 pi

#run-gridmix 0 10000
#run-gridmix 25 10000
#run-gridmix 50 10000
#run-gridmix 75 10000
#run-gridmix 100 10000
#
#run-gridmix 0 20000
#run-gridmix 25 20000
#run-gridmix 50 20000
#run-gridmix 75 20000
#run-gridmix 100 20000



