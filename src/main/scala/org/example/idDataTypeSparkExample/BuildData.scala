package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{column, expr, sequence}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, TypePath}
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.util.concurrent.Executors
import scala.collection.mutable

class BuildData(sparkSession: SparkSession) {
  private val log = LoggerFactory.getLogger(this.getClass)


  def buildSource(
    startId: Long,
    endId: Long,
    step: Long,
    repartitionWrite: Int,
    sourceTemplatePath: String,
    buildExplain: Boolean,
    buildSliceCount:Int
  ): Unit = {

    var start = startId
    val bData:mutable.Buffer[Row] = mutable.Buffer()
    var loops = 0
    val warnLoops = 50000
    while (startId <= start && start < endId ){
      loops+=1
      if (loops > warnLoops)
        log.warn("Too much loops")

      val end = if ( startId < start + buildSliceCount && start + buildSliceCount < endId ) start + buildSliceCount -1 else endId
      bData.append(Row(start, end, step))
      start = end + 1
    }

    val schema = StructType(Array(
      StructField(Columns._start_id, LongType, nullable = false),
      StructField(Columns._end_id, LongType, nullable = false),
      StructField(Columns._step, LongType, nullable = false),
    ))

    val dfPrepare =
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(bData), schema)
      .repartition(repartitionWrite)
      .cache()

    dfPrepare.show(1000,false)

    // elements due to exceeding the array size limit 2147483632.
  import org.apache.spark.sql.functions.{from_unixtime, lit}
    val broadcastVar = sparkSession.sparkContext.broadcast(new scala.util.Random)
    val df = dfPrepare.withColumn(
				s"${Columns._id_bigint}",
				sequence(
          column(s"${Columns._start_id}"),
          column(s"${Columns._end_id}"),
          column(s"${Columns._step}")))
      .selectExpr(s"explode (${Columns._id_bigint}) as ${Columns._id_bigint}")
      .withColumn(Columns._id_ts, from_unixtime(lit(broadcastVar.value.nextInt())).cast(TimestampType))
      .withColumn(Columns._id_uuid, expr("uuid()"))
      .withColumn(Columns._id_decimal, column(Columns._id_bigint).cast(DecimalType(19, 0)))
      .withColumn(Columns._id_string, column(Columns._id_bigint).cast(StringType))

      if(buildExplain) df.explain()

      df.write.parquet(sourceTemplatePath)

  }


  def writeDFs(
    sourceDataFrame: DataFrame,
    repartitionWrite: Int,
    buildSingleIdColumn: Boolean,
    writeThreadCount:Int,
    typePaths:Seq[TypePath]
  ): Unit = {
    sourceDataFrame.show(1000,false)
    println(s"""poll size $writeThreadCount""")
    val threadPool = Executors.newFixedThreadPool(writeThreadCount)

    typePaths.foreach(v => {
          Seq(v.pathLeft, v.pathRight).foreach(path =>
            threadPool.submit(
              new WriteRunner(
                path = path,
                dataFrame = renameTargetIdDF(sourceDataFrame, buildSingleIdColumn, v.logicalTypeName),
                repartitionWrite = repartitionWrite,
                compression = v.compression,
                fileFormat = v.fileFormat)
        )
      )
    })

    log.debug("ThreadPool submitted")
    threadPool.shutdown()
    import java.util.concurrent.TimeUnit
    try
      threadPool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    log.debug("ThreadPool shutdown")
  }

  private def renameTargetIdDF(
    sourceDataFrame:DataFrame,
    buildSingleIdColumn:Boolean,
    logicalDataTypeName:String
  ):DataFrame = {
    val sdfRenamed = sourceDataFrame
      .withColumnRenamed(s"${Columns._id}_$logicalDataTypeName",Columns._id)

    val cols = if (buildSingleIdColumn) Array{Columns._id} else sdfRenamed.columns

    sdfRenamed.select(cols.map(c=> column(c)):_*)
  }

  def statSize(typePaths: Seq[TypePath]): DataFrame = {

    val data = typePaths.map(v => {
      val sizeInfo = getSizeMB(v.pathLeft)
      Row(v.fileFormat,v.compression,v.logicalTypeName, sizeInfo._1,sizeInfo._2)
    })

    val schema = StructType(Array(

      StructField(s"${Columns._file_format}", StringType, nullable = false),
      StructField(s"${Columns._file_compression}", StringType, nullable = false),
      StructField(s"${Columns._type_name}", StringType, nullable = false),
      StructField(s"${Columns._size_mb}", DoubleType, nullable = false),
      StructField(s"${Columns._size_status}", StringType, nullable = false)
    ))

    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
  }

  private def getSizeMB(path: String): (Double, String) = {
    try {
      val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val cs = fs.getContentSummary(new Path(path))
      (cs.getLength / 1024 / 1024,"")
    }catch {
      case ex: FileNotFoundException =>
        log.warn("err", ex)
        val status = s"not found $path"
        (0d, status)
    }
  }
  private class WriteRunner(
    path: String,
    dataFrame: DataFrame,
    repartitionWrite: Int,
    compression:String,
    fileFormat:String
  ) extends Runnable {
    override def run(): Unit = {
      try {

        dataFrame
          .repartitionByRange(repartitionWrite, column(s"${Columns._id}"))
          .write
          .mode(SaveMode.ErrorIfExists)
          .format(fileFormat)
          .option("compression", compression)
          .option("header","true") //csv
          .save(s"$path")
      } catch {
        case ex: Throwable =>
          log.error(s"Error while executing thread $ex; compression $compression; fileFormat")
          Thread.currentThread().interrupt()
      }
    }
  }
}
