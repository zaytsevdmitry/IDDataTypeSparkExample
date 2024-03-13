package org.example.idDataTypeSparkExample

import org.apache.spark.sql.functions.{column, sequence}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class PrepareData(sparkSession: SparkSession) {



  def buildSource(startId: Long,
                  endId: Long,
                  step: Long): DataFrame = {

    val data = Seq(Row(startId, endId, step))
    val schema = StructType(Array(
      StructField("start_id", LongType, false),
      StructField("end_id", LongType, false),
      StructField("step", LongType, false),
    ))
    sparkSession
      .createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
      .withColumn("id", sequence(column("start_id"), column("end_id"), column("step")))
      .selectExpr("explode (id) as id")
  }

  def writeDFs(workDirectory: String,
               sourceDataFrame: DataFrame): Unit = {
    SharedVars.pathTypePairList.foreach(v=>{
      writeDF(s"${workDirectory}/${v._2.pathLeft}",sourceDataFrame,v._1)
      writeDF(s"${workDirectory}/${v._2.pathRight}",sourceDataFrame,v._1)
    })
  }

  def writeDF(path: String,
              dataFrame: DataFrame,
              strType: String,
              precision: Short = 0,
              scale: Short = 0): Unit = {

    val strTypeExpr =
      if (strType.equals(SharedVars._decimal))
        s"${SharedVars._decimal}(38,0)"
      else strType

    dataFrame
      .select(column("id").cast(s"$strTypeExpr"))
      .write
      .mode(SaveMode.ErrorIfExists)
      .parquet(s"$path")
  }
}
