# Agreements
test suit files must be placed in the same directory
IDDataTypeSparkExample.jar should be placed in the directory above 
```
 ../IDDataTypeSparkExample.jar \
```

# Files
* testcase.bash - runs the specified test case 
* testcase_coumn_all.csv - case with all columns  ( bigint,decimal, string-bigint,string-uid,timestamp )
* testcase_coumn_single.csv - all columns will be deleted except id

# Example
make the file executable
```
> chmod -x ./testcase.bash
```
```
> ./testcase.bash  testcase_coumn_all.csv
```
or 
```
> ./testcase.bash  testcase_coumn_single.csv
```

# Analise example
```
spark
.read
.parquet("/tmp/IDDataTypeSparkExample/testcase_coumn_all.csv/stat/analyze_stat_df")
.createOrReplaceTempView("analyze_stat_df")
```

```
spark.sql("""
with t as (
 select *
 ,  collect_list(duration_s) 
         over(
              partition by fileFormat
               , buildCompression
               , type_name 
               order by log_dt) sequence_duration
 from analyze_stat_df
)
select fileFormat
     , buildCompression
     , type_name
     , size_mb
     , round(size_mb_pcnt,1)size_mb_pcnt
     , min(duration_s) min_duration_s
     , max(duration_s) max_duration_s
     , avg(duration_s) avg_duration_s
     , max(sequence_duration) sequence_duration
from t
group by fileFormat
     , buildCompression
     , type_name
     , size_mb
     , size_mb_pcnt
order by avg_duration_s
""").show(200)
```
```
+----------+----------------+---------+-------+------------+--------------+--------------+--------------+--------------------+
|fileFormat|buildCompression|type_name|size_mb|size_mb_pcnt|min_duration_s|max_duration_s|avg_duration_s|   sequence_duration|
+----------+----------------+---------+-------+------------+--------------+--------------+--------------+--------------------+
|   parquet|            gzip|   string| 3263.0|       100.0|            48|            74|          54.5|    [48, 48, 74, 48]|
|   parquet|             lz4|   string| 5948.0|       100.0|            48|            67|         55.25|    [48, 50, 67, 56]|
|       orc|            zstd|   bigint| 2216.0|       100.0|            56|            65|          59.5|    [57, 56, 60, 65]|
|   parquet|          snappy|   bigint| 5808.0|       100.0|            58|            65|          61.0|    [59, 65, 62, 58]|
|   parquet|             lz4|   bigint| 5948.0|       100.0|            54|            72|          61.5|    [55, 65, 54, 72]|
|   parquet|          snappy|   string| 5808.0|       100.0|            54|            73|         62.75|    [54, 56, 68, 73]|
|       orc|             lzo|   string| 4333.0|       100.0|            52|            79|          63.5|    [79, 57, 52, 66]|
|   parquet|            zstd|   bigint| 2866.0|       100.0|            60|            70|          64.0|    [70, 65, 61, 60]|
|       orc|             lzo|   bigint| 4333.0|       100.0|            57|            74|          64.5|    [65, 57, 62, 74]|
|   parquet|            gzip|   bigint| 3263.0|       100.0|            59|            83|         65.75|    [60, 59, 83, 61]|
|   parquet|            zstd|   string| 2865.0|       100.0|            52|            74|          66.0|    [68, 74, 52, 70]|
|       orc|             lz4|   bigint| 4288.0|       100.0|            59|            78|          67.0|    [63, 59, 68, 78]|
|       orc|            zstd|   string| 2216.0|       100.0|            71|            75|         73.25|    [73, 71, 74, 75]|
|       orc|          snappy|   string| 4177.0|       100.0|            53|           105|         74.25|   [105, 62, 77, 53]|
|       orc|          snappy|   bigint| 4177.0|       100.0|            59|           114|         74.25|   [114, 59, 62, 62]|
|       orc|             lz4|   string| 4288.0|       100.0|            66|            97|          81.5|    [89, 66, 74, 97]|
|       orc|             lz4|     uuid| 4604.0|       107.4|            78|            87|          82.0|    [82, 81, 87, 78]|
|   parquet|             lz4|  decimal| 5949.0|       100.0|            74|            95|          83.0|    [95, 86, 77, 74]|
|   parquet|            gzip|  decimal| 3263.0|       100.0|            78|            94|          85.0|    [78, 81, 94, 87]|
|       orc|             lzo|     uuid| 4595.0|       106.0|            79|            95|         85.75|    [95, 84, 79, 85]|
|       orc|          snappy|     uuid| 4469.0|       107.0|            82|            92|          86.5|    [92, 85, 82, 87]|
|   parquet|             lz4|     uuid| 6023.0|       101.3|            81|            90|         86.75|    [89, 90, 87, 81]|
|   parquet|          snappy|     uuid| 5824.0|       100.3|            85|            93|          88.0|    [85, 86, 88, 93]|
|       orc|             lz4|  decimal| 4288.0|       100.0|            78|           106|          90.0|   [87, 78, 106, 89]|
|   parquet|          snappy|  decimal| 5808.0|       100.0|            74|           108|         90.25|  [74, 75, 108, 104]|
|   parquet|            gzip|     uuid| 3367.0|       103.2|            85|            97|          91.0|    [85, 89, 97, 93]|
|       orc|          snappy|  decimal| 4177.0|       100.0|            87|           100|          93.0|   [100, 95, 90, 87]|
|   parquet|            zstd|     uuid| 3189.0|       111.3|            87|           116|          97.5|   [116, 92, 95, 87]|
|       orc|             lzo|  decimal| 4333.0|       100.0|            80|           152|         106.0|  [109, 80, 83, 152]|
|   parquet|            zstd|  decimal| 2866.0|       100.0|            86|           125|        109.25| [113, 113, 86, 125]|
|       orc|            zstd|     uuid| 2592.0|       117.0|            92|           160|         110.5|   [92, 97, 160, 93]|
|       orc|            zstd|  decimal| 2216.0|       100.0|           109|           124|         117.5|[109, 120, 124, 117]|
+----------+----------------+---------+-------+------------+--------------+--------------+--------------+--------------------+

```