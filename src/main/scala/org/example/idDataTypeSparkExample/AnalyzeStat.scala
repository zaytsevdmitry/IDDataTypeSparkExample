package org.example.idDataTypeSparkExample

import org.apache.spark.sql.functions.column
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, ConfigKeys, Constants}

import java.sql.Timestamp
import java.util.Date

class AnalyzeStat(sparkSession: SparkSession) {


  def analyzeStat(dataStatDF: DataFrame,
                  joinTestDF: DataFrame,
                  fileFormatName:String,
                  compressionName:String
                 ): DataFrame = {

    dataStatDF
      .filter(s"${Columns._type_name} = '${Constants._bigint}'")
      .select(column(s"${Columns._size_mb}").as(s"${Columns._size_mb_target}"))
      .crossJoin(dataStatDF)
      .createOrReplaceTempView("data_stat")
    sparkSession.sql("select * from data_stat").show()
    joinTestDF
      .filter(s"${Columns._type_name} = '${Constants._bigint}'")
      .select(column(s"${Columns._duration_s}").as(s"${Columns._duration_target}"))
      .crossJoin(joinTestDF)
      .createOrReplaceTempView("join_test")

    val data = Seq(Row(fileFormatName, compressionName, new Timestamp(new Date().getTime)))
    val schema = StructType(Array(
      StructField(ConfigKeys.fileFormat, StringType, nullable = false),
      StructField(ConfigKeys.buildCompression, StringType, nullable = false),
      StructField(Columns._log_dt, TimestampType, nullable = false),
    ))

    val logParamsDf = sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)

    logParamsDf.crossJoin(
      sparkSession.sql(
        s"""
           |select ${Columns._type_name}
           |     , ${Columns._size_mb}
           |     , case when ${Columns._size_mb_target} != 0
           |            then ${Columns._size_mb} / ${Columns._size_mb_target} * 100
           |            else 0
           |        end as ${Columns._size_mb}_pcnt
           |     , ${Columns._duration_s}
           |     , case when ${Columns._duration_target} != 0
           |            then ${Columns._duration_s} / ${Columns._duration_target} * 100
           |            else 0
           |        end as ${Columns._duration_s}_pcnt
           |from data_stat d
           | join join_test j
           |   using(${Columns._type_name})
           |""".stripMargin)
    ).orderBy(Columns._type_name)
  }
}
