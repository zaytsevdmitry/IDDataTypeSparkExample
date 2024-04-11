#!/bin/bash
workDirectoryRoot=/tmp/IDDataTypeSparkExample

hdfs dfs -rm -r -skipTrash $workDirectoryRoot

for line in $(awk 'NR>1' ./testcase.csv| tr -d ' '); # skip first line and drop spaces
do
  IFS=$';'; split=($line); unset IFS;
  # $split is now a bash array

  fileFormat=${split[0]}
  buildData=${split[1]}
  buildRangeStartId=${split[2]}
  buildRangeEndId=${split[3]}
  buildRangeStep=${split[4]}
  buildCached=${split[5]}
  buildRepartition=${split[6]}
  buildCompression=${split[7]}
  buildExplain=${split[8]}
  testJoins=${split[9]}
  testJoinsExplain=${split[10]}
  waitForUser=${split[11]}

# 20gb need to generate 100000001 rows
  CMD="spark-submit \
   --conf spark.executor.memory=20g \
   --conf spark.executor.cores=1 \
   --conf spark.driver.host=`hostname -i` \
   --conf spark.ui.showConsoleProgress=true \
   --conf spark.sql.autoBroadcastJoinThreshold=-1 \
   --class  org.example.idDataTypeSparkExample.Main \
   ../IDDataTypeSparkExample.jar \
  workDirectory=$workDirectoryRoot/$fileFormat/$buildCompression \
  fileFormat=$fileFormat \
  buildData=$buildData \
  buildRangeStartId=$buildRangeStartId \
  buildRangeEndId=$buildRangeEndId \
  buildRangeStep=$buildRangeStep \
  buildCached=$buildCached \
  buildRepartition=$buildRepartition \
  buildCompression=$buildCompression \
  buildExplain=$buildExplain \
  testJoins=$testJoins \
  testJoinsExplain=$testJoinsExplain \
  waitForUser=$waitForUser \
  logStatDir=$workDirectoryRoot/stat"
  echo $CMD
  $CMD
done



