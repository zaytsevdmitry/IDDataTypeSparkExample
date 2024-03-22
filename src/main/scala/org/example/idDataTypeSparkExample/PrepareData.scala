package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{column, expr, sequence}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, Constants}

class PrepareData(sparkSession: SparkSession) {

  val defaultCompression = "snappy"
  def buildSource(
    startId: Long,
    endId: Long,
    step: Long,
    cached: Boolean
  ): DataFrame = {

    val data = Seq(Row(startId, endId, step))
    val schema = StructType(Array(
      StructField("start_id", LongType, nullable = false),
      StructField("end_id", LongType, nullable = false),
      StructField("step", LongType, nullable = false),
    ))
    // elements due to exceeding the array size limit 2147483632.
    val df = sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
      .withColumn(
				s"${Columns._id}",
				sequence(column("start_id"), column("end_id"), column("step")))
      .selectExpr("explode (id) as id")
      .select(column(s"${Columns._id}"), expr("uuid()").as("id_1"))

    if (cached) df.cache()
    else df
  }

  def writeDFs(
    workDirectory: String,
    sourceDataFrame: DataFrame,
    repartitionWrite: Int,
    compression:String): Unit = {
    Constants.pathTypePairList(workDirectory).foreach(v => {
      writeDF(v._2.pathLeft, sourceDataFrame, v._1, repartitionWrite, compression)
      writeDF(v._2.pathRight, sourceDataFrame, v._1, repartitionWrite, compression)
    })
  }

  private def writeDF(
    path: String,
    dataFrame: DataFrame,
    strType: String,
    repartitionWrite: Int,
    compression:String): Unit = {

    val c = if (compression == null) defaultCompression else compression

    val df = if (strType.equals(Constants._decimal)) {
      dataFrame
        .select(column(s"${Columns._id}")
          .cast(DecimalType(19, 0))) // length of max long
    } else if (strType.equals(Constants._uuid)) {
      dataFrame
        .drop(s"${Columns._id}")
        .withColumnRenamed(s"${Columns._id_1}", s"${Columns._id}")
    } else {
      dataFrame
        .select(column(s"${Columns._id}").cast(strType))
    }

    df.repartitionByRange(repartitionWrite, column(s"${Columns._id}"))
      .write
      .mode(SaveMode.ErrorIfExists)
      .option("compression", c)
      .parquet(s"$path")
  }

  def statSize(workDirectory: String): DataFrame = {

    val data = Constants.pathTypePairList(workDirectory).map(v => {
      Row(v._1, getSizeMB(v._2.pathLeft))
    })

    val schema = StructType(Array(
      StructField(s"${Columns._type_name}", StringType, nullable = false),
      StructField(s"${Columns._size_mb}", DoubleType, nullable = false),
    ))

    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  private def getSizeMB(path: String): Double = {
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val cs = fs.getContentSummary(new Path(path))
    cs.getLength / 1024 / 1024
  }
}
