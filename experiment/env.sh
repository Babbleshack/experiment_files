HADOOP_VERSION=${HADOOP_VERSION:-"3.1.2"}
#See Gridmix docs: https://hadoop.apache.org/docs/r2.8.5/hadoop-gridmix/GridMix.html
export HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-rumen-$HADOOP_VERSION.jar
export GRIDMIX="/opt/hadoop/share/hadoop/tools/lib/hadoop-gridmix-$HADOOP_VERSION.jar -libjars /opt/hadoop/share/hadoop/tools/lib/hadoop-rumen-$HADOOP_VERSION.jar"
#See Rumen docs: https://hadoop.apache.org/docs/current/hadoop-rumen/Rumen.html
export RUMEN="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-rumen-$HADOOP_VERSION.jar"
export RUMEN_TB="$RUMEN org.apache.hadoop.tools.rumen.TraceBuilder"
export RUMEN_FLD="$RUMEN org.apache.hadoop.tools.rumen.Folder"
export HADOOP_LOG="/tmp/hadoop-yarn/staging/history/done"
