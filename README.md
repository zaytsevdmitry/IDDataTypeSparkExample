# IDDataTypeSparkExample

## Command line attributes

### workDirectory
* type:String
* Directory where testing data is created

### fileFormat
* type:String
* Data file format (like parquet/orc etc)

### buildData 
* type:Boolean
* build test data
* values:true/false

### buildRangeStartId
* type:Long
* The value from which the generation of identifiers will begin.

### buildRangeEndId
* type:Long
* The value up to which identifiers were generated.

### buildRangeStep
* type:Int
* The step with which identifiers are generated

### buildCached
* type: Boolean
* If you do not use caching, the generation will be repeated for each data type. If you have available RAM, it is better to enable the use of cache, otherwise you can disable.
* values:true/false

### buildRepartition
* type: Int
* Affects the number of parts of the dataset. This is reflected in the number of files when written to disk.

### buildCompression
* type: String
* Type of compression codec
* values: snappy, gzip, lzo, brotli, lz4, zstd.

### buildExplain
* type:Boolean
* Print test data generation explanation plan
* values:true/false

### testJoins
* type: Boolean
* Test the join of datasets
* values:true/false

### testJoinsExplain
* type: Boolean
* Print test join explanation plan
* values:true/false

### waitForUser
* type: Boolean
* Wait after the application has completed input for an explicit request to terminate the application.
  Necessary for cases when the user wants to view the spark-ui
* values:true/false

### logStatDir
* type:String
* The location where the dataset with statistics will wrote. The writing is made in the parquet format using the append method.

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
