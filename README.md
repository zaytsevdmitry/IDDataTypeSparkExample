# IDDataTypeSparkExample

This is application for testing sorting/formats/compressions options of data files.
Usage options:
* Building test data sets
* Running test queries on data
* Both options in one run


You can disable data creation using the buildData=false attribute.
This is useful when you need to repeat a query on data several times.

You can turn off queries using the testJoins=false parameter.
Useful when you need to get data only on storage sizes.

## Building test data sets
An application using the build* attributes to build the following data set in memory.
Schema:
```
root
 |-- id_bigint: long (nullable = false)
 |-- id_row_num: integer (nullable = false)
 |-- id_ts: timestamp (nullable = false)
 |-- id_uuid: string (nullable = false)
 |-- id_decimal: decimal(19,0) (nullable = true)
 |-- id_string: string (nullable = false)
```

For example, when specifying a range of values 1-20 in increments of 1, the set will look like this:
```
+---------+----------+-------------------+--------------------+----------+---------+
|id_bigint|id_row_num|              id_ts|             id_uuid|id_decimal|id_string|
+---------+----------+-------------------+--------------------+----------+---------+
|        1|         1|1970-01-01 03:00:01|31197708-435d-44c...|         1|        1|
|        2|         2|1970-01-01 03:00:02|3ee21723-f89b-41f...|         2|        2|
|        3|         3|1970-01-01 03:00:03|21de1c9b-89b7-42f...|         3|        3|
|        4|         4|1970-01-01 03:00:04|73c7800a-cad2-460...|         4|        4|
|        5|         5|1970-01-01 03:00:05|68aa3846-da19-468...|         5|        5|
|        6|         6|1970-01-01 03:00:06|e73f8013-0cbe-4dc...|         6|        6|
|        7|         7|1970-01-01 03:00:07|4ce9c99a-a0ac-47d...|         7|        7|
|        8|         8|1970-01-01 03:00:08|92c22465-5279-478...|         8|        8|
|        9|         9|1970-01-01 03:00:09|d16c490a-e286-472...|         9|        9|
|       10|        10|1970-01-01 03:00:10|2f6fa20c-aa6e-42b...|        10|       10|
|       11|        11|1970-01-01 03:00:11|88f28e06-1857-4b8...|        11|       11|
|       12|        12|1970-01-01 03:00:12|397d6a5b-1125-482...|        12|       12|
|       13|        13|1970-01-01 03:00:13|8a13858c-6d74-469...|        13|       13|
|       14|        14|1970-01-01 03:00:14|4043599c-af7e-437...|        14|       14|
|       15|        15|1970-01-01 03:00:15|150a5fba-9f64-458...|        15|       15|
|       16|        16|1970-01-01 03:00:16|bd45f30a-33fd-47a...|        16|       16|
|       17|        17|1970-01-01 03:00:17|ee330e68-aa36-48f...|        17|       17|
|       18|        18|1970-01-01 03:00:18|43d75fa2-a3d4-47c...|        18|       18|
|       19|        19|1970-01-01 03:00:19|9c111d82-73c7-432...|        19|       19|
|       20|        20|1970-01-01 03:00:20|25216564-83dc-43a...|        20|       20|
+---------+----------+-------------------+--------------------+----------+---------+
```
The data sets of each option are saved in two identical copies, to subsequently simulate the connection of two different tables.

### Final dataset control
The final composition of the columns of the saved datasets is determined by the buildSingleIdColumn attribute.
If you need to estimate the pure time spent working with keys or their size on disk, enable this attribute buildSingleIdColumn=true
All extra columns will be dropped, except for the column named ID.
Disabling the attribute will allow you to simulate a test on a data composition similar to those stored in a real product environment.

### Limits
* The target number of dataset rows should not exceed MAX_INT, otherwise you may receive the error "elements due to exceeding the array size limit 2147483632." 
* When specifying a wide range, make sure there is enough memory for the executor in yarn mode or the driver in local mode

## Running test queries on data

Example Query Plan

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [id#65L, id_row_num#66, id_ts#67, id_uuid#68, id_decimal#69, id_string#70, id_row_num#78, id_ts#79, id_uuid#80, id_decimal#81, id_string#82]
   +- SortMergeJoin [id#65L], [id#77L], Inner
      :- Sort [id#65L ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(id#65L, 200), ENSURE_REQUIREMENTS, [plan_id=57]
      :     +- Filter isnotnull(id#65L)
      :        +- FileScan parquet [id#65L,id_row_num#66,id_ts#67,id_uuid#68,id_decimal#69,id_string#70] Batched: true, DataFilters: [isnotnull(id#65L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[/tmp/IDDataTypeSparkExample/testcase_coumn_all.csv/parqu..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,id_row_num:int,id_ts:timestamp,id_uuid:string,id_decimal:decimal(19,0),id_string...
      +- Sort [id#77L ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(id#77L, 200), ENSURE_REQUIREMENTS, [plan_id=58]
            +- Filter isnotnull(id#77L)
               +- FileScan parquet [id#77L,id_row_num#78,id_ts#79,id_uuid#80,id_decimal#81,id_string#82] Batched: true, DataFilters: [isnotnull(id#77L)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[/tmp/IDDataTypeSparkExample/testcase_coumn_all.csv/parqu..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint,id_row_num:int,id_ts:timestamp,id_uuid:string,id_decimal:decimal(19,0),id_string...
```

The test is focused on connecting two large datasets. The Spark optimizer may mistakenly apply BroadcastJoin (the case of orc + bigint is very small on disk). This may cause the driver memory overflow.
Therefore BroadcastJoin needs to be disabled.
```
--conf spark.sql.autoBroadcastJoinThreshold=-1 
```

  
## Statistics
The analytical set is formed using the "append" method after all queries have been completed
The storage location is determined by the logStatDir attribute
```
root
 |-- fileFormat: string (nullable = true)
 |-- buildCompression: string (nullable = true)
 |-- log_dt: timestamp (nullable = true)
 |-- type_name: string (nullable = true)
 |-- size_mb: double (nullable = true)
 |-- size_mb_pcnt: double (nullable = true)
 |-- duration_s: long (nullable = true)
 |-- duration_s_pcnt: double (nullable = true)

```
* log_dt - logging date
* size_mb - dataset size on disk
* size_mb_pcnt - the ratio of the size of the occupied data in relation to the similar one in the BigInt type
* duration_s - duration of the test query in seconds
* duration_s_pcnt - relation to the time of a similar request using the BigInt type

```
+----------+----------------+--------------------+---------+-------+------------------+----------+------------------+
|fileFormat|buildCompression|              log_dt|type_name|size_mb|      size_mb_pcnt|duration_s|   duration_s_pcnt|
+----------+----------------+--------------------+---------+-------+------------------+----------+------------------+
|   parquet|            gzip|2024-04-12 00:17:...|   bigint| 3263.0|             100.0|        83|             100.0|
|   parquet|            gzip|2024-04-12 00:06:...|   bigint| 3263.0|             100.0|        60|             100.0|
|   parquet|            gzip|2024-04-12 00:23:...|   bigint| 3263.0|             100.0|        61|             100.0|
|   parquet|            gzip|2024-04-12 00:11:...|   bigint| 3263.0|             100.0|        59|             100.0|
|   parquet|            gzip|2024-04-12 00:23:...|  decimal| 3263.0|             100.0|        87|142.62295081967213|
|   parquet|            gzip|2024-04-12 00:11:...|  decimal| 3263.0|             100.0|        81|137.28813559322032|
|   parquet|            gzip|2024-04-12 00:17:...|  decimal| 3263.0|             100.0|        94|113.25301204819279|
|   parquet|            gzip|2024-04-12 00:06:...|  decimal| 3263.0|             100.0|        78|             130.0|
|   parquet|            gzip|2024-04-12 00:06:...|   string| 3263.0|             100.0|        48|              80.0|
|   parquet|            gzip|2024-04-12 00:11:...|   string| 3263.0|             100.0|        48| 81.35593220338984|
|   parquet|            gzip|2024-04-12 00:23:...|   string| 3263.0|             100.0|        48| 78.68852459016394|
|   parquet|            gzip|2024-04-12 00:17:...|   string| 3263.0|             100.0|        74|  89.1566265060241|
|   parquet|            gzip|2024-04-12 00:11:...|     uuid| 3367.0|103.18725099601593|        89|150.84745762711864|
|   parquet|            gzip|2024-04-12 00:23:...|     uuid| 3367.0|103.18725099601593|        93|152.45901639344262|
|   parquet|            gzip|2024-04-12 00:17:...|     uuid| 3367.0|103.18725099601593|        97|116.86746987951808|
|   parquet|            gzip|2024-04-12 00:06:...|     uuid| 3367.0|103.18725099601593|        85|141.66666666666669|
|   parquet|          snappy|2024-04-11 22:58:...|   bigint| 5808.0|             100.0|        65|             100.0|
|   parquet|          snappy|2024-04-11 23:11:...|   bigint| 5808.0|             100.0|        58|             100.0|
|   parquet|          snappy|2024-04-11 22:53:...|   bigint| 5808.0|             100.0|        59|             100.0|
|   parquet|          snappy|2024-04-11 23:05:...|   bigint| 5808.0|             100.0|        62|             100.0|
+----------+----------------+--------------------+---------+-------+------------------+----------+------------------+
only showing top 20 rows
```
## Command line attributes

### workDirectory
* type:String
* Directory where testing data is created

### fileFormat
* type:String
* Data file format (like parquet/orc etc)

### buildData 
* type:Boolean
* building of test data sets
* values:true/false

### buildRangeStartId
* type:Long
* start of identifiers generation.

### buildRangeEndId
* type:Long
* end of identifiers generation

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

### buildSingleIdColumn
* type:Boolean
* All columns will be deleted except id. If false  all columns will be saved (bigint, decimal, string-bigint, string-uid, timestamp)
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

## Example command line
```
> spark-submit \
 --conf spark.executor.memory=20g \ # 20gb need to generate 100000001 rows
 --conf spark.executor.cores=1 \ 
 --conf spark.driver.host=\`hostname -i\` \
 --conf spark.ui.showConsoleProgress=true \  # 
 --class  org.example.idDataTypeSparkExample.Main IDDataTypeSparkExample.jar \
workDirectory=/tmp/IDDataTypeSparkExample \
fileFormat=parquet \
buildData=true \
buildRangeStartId=9223372036754775807 \
buildRangeEndId=9223372036854775807 \
buildRangeStep=1 \
buildCached=true \
buildRepartition=10 \
buildCompression=none \
buildExplain=true \
buildSingleIdColumn=true \
testJoins=true \
testJoinsExplain=true \
waitForUser=true 
```
