package org.example.idDataTypeSparkExample

import org.apache.spark.sql.functions.{column, lit, make_interval}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Date

class JoinTest(sparkSession: SparkSession) {


  private def join(workDirectory: String,
           typePath:SharedVars.TypePath):DataFrame = {
    val left = sparkSession.read.parquet(s"$workDirectory/${typePath.pathLeft}")
    val right = sparkSession.read.parquet(s"$workDirectory/${typePath.pathRight}")
    left.join(right,Array("id"))
  }


  def runJoins(workDirectory: String): DataFrame = {
    val resultList = SharedVars.pathTypePairList.map(t => {

      val df = join(workDirectory, t._2)
      df.explain()
      val testStartTime = new Date().getTime
      val count = df.count()
      val testEndTime = new Date().getTime

      Row(t._1, (testEndTime - testStartTime) , count)
    }
    )
    val schema = StructType(Array(
      StructField("type_name", StringType, false),
      StructField("duration", LongType , false),
      StructField("count_rows", LongType, false),
    ))
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(resultList), schema)
      .withColumn(
        "duration",
        (make_interval(lit(0),lit(0),lit(0),lit(0),lit(0),lit(0),column("duration")/1000))
      )
      .orderBy("type_name")
  }
}
