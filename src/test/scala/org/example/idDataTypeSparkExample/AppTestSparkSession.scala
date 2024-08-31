package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession

object AppTestSparkSession{

	private val sparkSession = SparkSession
		.builder()
		.appName("unit-tests")
		.config("spark.master", "local[*]")
		.config("spark.driver.memory", "1g")
		.config("spark.sql.shuffle.partitions", "1")
		.config("spark.rdd.compress", "false")
		.config("spark.shuffle.compress", "false")
		.config("spark.dynamicAllocation.enabled", "false")
		.config("spark.ui.enabled", "true")
		//.config("spark.ui.port", "4050")
		.config("spark.ui.showConsoleProgress", "true")
		.getOrCreate()


	def getSpark: SparkSession = sparkSession
}
