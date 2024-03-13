package org.example.idDataTypeSparkExample

import org.apache.spark.sql.functions.{column, sequence}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.Constants

class PrepareData(sparkSession: SparkSession){


	def buildSource(
		startId: Long,
		endId: Long,
		step: Long): DataFrame = {

		val data = Seq(Row(startId, endId, step))
		val schema = StructType(Array(
			StructField("start_id", LongType, nullable = false),
			StructField("end_id", LongType, nullable = false),
			StructField("step", LongType, nullable = false),
		))
		// elements due to exceeding the array size limit 2147483632.
		sparkSession
			.createDataFrame(sparkSession.sparkContext.parallelize(data), schema)
			.withColumn("id", sequence(column("start_id"), column("end_id"), column("step")))
			.selectExpr("explode (id) as id")
	}

	def writeDFs(
		workDirectory: String,
		sourceDataFrame: DataFrame): Unit = {
		Constants.pathTypePairList.foreach(v => {
			writeDF(s"$workDirectory/${v._2.pathLeft}", sourceDataFrame, v._1)
			writeDF(s"$workDirectory/${v._2.pathRight}", sourceDataFrame, v._1)
		})
	}

	private def writeDF(
		path: String,
		dataFrame: DataFrame,
		strType: String): Unit = {

		val strTypeExpr =
			if (strType.equals(Constants._decimal))
				s"${Constants._decimal}(38,0)"
			else strType

		dataFrame
			.select(column("id").cast(s"$strTypeExpr"))
			.write
			.mode(SaveMode.ErrorIfExists)
			.parquet(s"$path")
	}
}
