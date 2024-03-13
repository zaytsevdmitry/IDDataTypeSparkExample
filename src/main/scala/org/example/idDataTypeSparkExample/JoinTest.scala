package org.example.idDataTypeSparkExample

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.example.idDataTypeSparkExample.shared.Constants

import java.util.Date

class JoinTest(sparkSession: SparkSession){


	def runJoins(workDirectory: String): DataFrame = {
		val resultList = Constants.pathTypePairList.map(t => {

			val df = join(workDirectory, t._2)
			df.explain()
			val testStartTime = new Date().getTime
			val count = df.count()
			val testEndTime = new Date().getTime

			Row(t._1, testEndTime - testStartTime, count)
		}
		)
		val schema = StructType(Array(
			StructField("type_name", StringType, nullable = false),
			StructField("duration", LongType, nullable = false),
			StructField("count_rows", LongType, nullable = false),
		))
		sparkSession
			.createDataFrame(sparkSession.sparkContext.parallelize(resultList), schema)
			.orderBy("type_name")
	}

	private def join(
		workDirectory: String,
		typePath: Constants.TypePath): DataFrame = {
		val left = sparkSession.read.parquet(s"$workDirectory/${typePath.pathLeft}")
		val right = sparkSession.read.parquet(s"$workDirectory/${typePath.pathRight}")
		left.join(right, Array("id"))
	}
}
