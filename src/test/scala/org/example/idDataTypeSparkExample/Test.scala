package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}

abstract class Test
	extends AnyFlatSpec
		with BeforeAndAfter
		with BeforeAndAfterEach
		with BeforeAndAfterAll{
	implicit val sparkSession: SparkSession = AppTestSparkSession.getSpark
}
