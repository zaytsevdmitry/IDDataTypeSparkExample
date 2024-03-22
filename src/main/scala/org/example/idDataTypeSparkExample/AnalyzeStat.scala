package org.example.idDataTypeSparkExample

import org.apache.spark.sql.functions.column
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, Constants}

class AnalyzeStat(sparkSession: SparkSession) {
  def analyzeStat(dataStatDF: DataFrame,
                  joinTestDF: DataFrame): DataFrame = {

    dataStatDF
      .filter(s"${Columns._type_name} = '${Constants._bigint}'")
      .select(column(s"${Columns._size_mb}").as(s"${Columns._size_mb_100_pcnt}"))
      .crossJoin(dataStatDF)
      .createOrReplaceTempView("data_stat")

    joinTestDF
      .filter(s"${Columns._type_name} = '${Constants._bigint}'")
      .select(column(s"${Columns._duration}").as(s"${Columns._duration_100_pcnt}"))
      .crossJoin(joinTestDF)
      .createOrReplaceTempView("join_test")

    sparkSession.sql(
      s"""
         |select ${Columns._type_name}
         |     , ${Columns._size_mb}
         |     , ${Columns._size_mb} / ${Columns._size_mb_100_pcnt} * 100 as ${Columns._size_mb}_pcnt
         |     , ${Columns._duration}
         |     , ${Columns._duration} / ${Columns._duration_100_pcnt} * 100 as ${Columns._duration}_pcnt
         |from data_stat d
         | join join_test j
         |   using(${Columns._type_name})
         |""".stripMargin)
      .orderBy(Columns._type_name)
  }
}
