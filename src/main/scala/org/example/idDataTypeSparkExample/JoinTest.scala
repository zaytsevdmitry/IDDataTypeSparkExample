package org.example.idDataTypeSparkExample

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.example.idDataTypeSparkExample.shared.{Columns, TypePath}
import org.slf4j.LoggerFactory

import java.sql.Timestamp
import java.util.Date

class JoinTest(sparkSession: SparkSession) {

  private val log = LoggerFactory.getLogger(this.getClass)

  def runJoins(typePaths: Seq[TypePath]): DataFrame = {

    val resultList = typePaths.map(join)

    val schema = StructType(Array(
      StructField(Columns._file_format, StringType, nullable = false),
      StructField(Columns._file_compression, StringType, nullable = false),
      StructField(Columns._type_name, StringType, nullable = false),
      StructField(Columns._duration_s, LongType, nullable = false),
      StructField(Columns._count_rows, LongType, nullable = false),
      StructField(Columns._count_distinct_id, LongType, nullable = false),
      StructField(Columns._avg_id, StringType, nullable = true),
      StructField(Columns._log_dt, TimestampType, nullable = false),
      StructField(Columns._log_dt_end, TimestampType, nullable = false),
      StructField(Columns._join_status, StringType, nullable = false)
    ))
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(resultList), schema)
      .orderBy(Columns._log_dt)
  }

  private def join(typePath:TypePath): Row = {
    val testStartTime = new Date().getTime
    try {
      println(s"""pathLeft:${typePath.pathLeft}\npathRight=${typePath.pathRight}""")
      val left = sparkSession.read.format(typePath.fileFormat).option("header",true).load(typePath.pathLeft)
      val right = sparkSession.read.format(typePath.fileFormat).option("header",true).load(typePath.pathRight)
      val df = left.join(right, Array(s"${Columns._id}"))
      df.explain()

      val count = df.count()
      val countDistinct = df.select(s"${Columns._id}").distinct().count()
      val avg = df.agg(functions.avg(s"${Columns._id}").cast(StringType)).first().getString(0)
      val testEndTime = new Date().getTime
      Row(
        typePath.fileFormat,
        typePath.compression,
        typePath.logicalTypeName,
        (testEndTime - testStartTime)/1000,
        count,
        countDistinct,
        avg,
        new Timestamp(testStartTime),
        new Timestamp(testEndTime),
        "ok"
      )

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        println(typePath)
        log.error(s"Error", ex)
        Row(
          typePath.fileFormat,
          typePath.compression,
          typePath.logicalTypeName,
          0L,//(testEndTime - testStartTime)/1000,
          0L,//count,
          0L,//countDistinct,
          "0",//avg,
          new Timestamp(testStartTime),
          new Timestamp(new Date().getTime),
          "error"
        )
    }
  }
}
