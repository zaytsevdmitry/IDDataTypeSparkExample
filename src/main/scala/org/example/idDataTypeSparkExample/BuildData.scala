package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{column, expr, row_number, sequence}
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, Constants}

class BuildData(sparkSession: SparkSession) {

  val defaultCompression = "snappy"

  def buildSource(
    startId: Long,
    endId: Long,
    step: Long,
    repartitionWrite: Int,
    cached: Boolean
  ): DataFrame = {

    val data = Seq(Row(startId, endId, step))
    val schema = StructType(Array(
      StructField(Columns._start_id, LongType, nullable = false),
      StructField(Columns._end_id, LongType, nullable = false),
      StructField(Columns._step, LongType, nullable = false),
    ))
    // elements due to exceeding the array size limit 2147483632.
    val windowSpec  = Window.orderBy(Columns._id_bigint)

    val df = sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
      .withColumn(
				s"${Columns._id_bigint}",
				sequence(
          column(s"${Columns._start_id}"),
          column(s"${Columns._end_id}"),
          column(s"${Columns._step}")))
      .selectExpr(s"explode (${Columns._id_bigint}) as ${Columns._id_bigint}")
      .repartition(repartitionWrite)
      .withColumn(Columns._id_row_num, row_number.over(windowSpec))
      .withColumn(Columns._id_ts, column(Columns._id_row_num).cast(TimestampType))
      .withColumn(Columns._id_uuid, expr("uuid()"))
      .withColumn(Columns._id_decimal, column(Columns._id_bigint).cast(DecimalType(19, 0)))
      .withColumn(Columns._id_string, column(Columns._id_bigint).cast(StringType))

    if (cached) df.cache()
    else df
  }

  def writeDFs(
    workDirectory: String,
    sourceDataFrame: DataFrame,
    repartitionWrite: Int,
    compression:String,
    fileFormat:String,
    buildSingleIdColumn:Boolean
  ): Unit = {


    Constants.pathTypePairList(workDirectory).foreach(v => {
      val sdfRenamed = sourceDataFrame
        .withColumnRenamed(s"${Columns._id}_${v._1}",Columns._id)

      val cols = if (buildSingleIdColumn) Array{Columns._id} else sdfRenamed.columns

      val sdf = sdfRenamed.select(cols.map(c=> column(c)):_*)

      writeDF(v._2.pathLeft, sdf, repartitionWrite, compression, fileFormat)
      writeDF(v._2.pathRight, sdf, repartitionWrite, compression, fileFormat)
    })
  }

  private def writeDF(
    path: String,
    dataFrame: DataFrame,
    repartitionWrite: Int,
    compression:String,
    fileFormat:String): Unit = {

    val c = if (compression == null) defaultCompression else compression

    dataFrame
      .repartitionByRange(repartitionWrite, column(s"${Columns._id}"))
      .write
      .mode(SaveMode.ErrorIfExists)
      .format(fileFormat)
      .option("compression", c)
      .save(s"$path")
  }
  private def dropColumns(
    dataFrame: DataFrame):DataFrame = {

    dataFrame.drop(dataFrame.columns.filter(c => c.equals(Columns._id)):_*)
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
