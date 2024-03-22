package org.example.idDataTypeSparkExample

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.example.idDataTypeSparkExample.shared.{Columns, Constants}

import java.util.Date

class JoinTest(sparkSession: SparkSession) {


  def runJoins(workDirectory: String): DataFrame = {
    val resultList = Constants.pathTypePairList(workDirectory).map(t => {

      val df = join(t._2)
      df.explain()
      val testStartTime = new Date().getTime
      val count = df.count()
      val countDistinct = df.select(s"${Columns._id}").distinct().count()
      val avg = df.agg(functions.avg(s"${Columns._id}").cast(StringType)).first().getString(0)
      val testEndTime = new Date().getTime

      Row(t._1, testEndTime - testStartTime, count, countDistinct, avg)
    }
    )
    val schema = StructType(Array(
      StructField(s"${Columns._type_name}", StringType, nullable = false),
      StructField(s"${Columns._duration}", LongType, nullable = false),
      StructField(s"${Columns._count_rows}", LongType, nullable = false),
      StructField(s"${Columns._count_distinct_id}", LongType, nullable = false),
      StructField(s"${Columns._avg_id}", StringType, nullable = true),
    ))
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(resultList), schema)
      .orderBy("type_name")
  }

  private def join(typePath: Constants.TypePath): DataFrame = {
    val left = sparkSession.read.parquet(typePath.pathLeft)
    val right = sparkSession.read.parquet(typePath.pathRight)
    left.join(right, Array(s"${Columns._id}"))
  }
}
