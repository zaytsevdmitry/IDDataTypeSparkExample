# IDDataTypeSparkExample

## Exapple command line
```
> spark-submit \
 --conf spark.executor.memory=20g \ # 20gb need to generate 100000001 rows
 --conf spark.executor.cores=1 \ 
 --conf spark.driver.host=\`hostname -i\` \
 --conf spark.ui.showConsoleProgress=true \  # 
 --class  org.example.idDataTypeSparkExample.Main IDDataTypeSparkExample.jar \
workDirectory=/tmp/IDDataTypeSparkExample
fileFormat=parquet
buildData=true
buildRangeStartId=9223372036754775807
buildRangeEndId=9223372036854775807
buildRangeStep=1
buildCached=true
buildRepartition=10
buildCompression=none
buildExplain=true
testJoins=true
testJoinsExplain=true
waitForUser=true
```
