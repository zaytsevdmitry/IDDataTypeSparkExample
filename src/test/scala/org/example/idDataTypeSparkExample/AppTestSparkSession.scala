package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession

object AppTestSparkSession{

	private val sparkSession = SparkSession
		.builder()
		.appName("unit-tests")
		.config("spark.master", "local[1]")
		.config("spark.driver.memory", "1g")
		.config("spark.sql.shuffle.partitions", "1")
		.config("spark.rdd.compress", "false")
		.config("spark.shuffle.compress", "false")
		.config("spark.dynamicAllocation.enabled", "false")
		.config("spark.ui.enabled", "false")
		.config("spark.ui.showConsoleProgress", "true")
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")

	def getSpark: SparkSession = sparkSession
}
