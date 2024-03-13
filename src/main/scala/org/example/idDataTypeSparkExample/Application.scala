package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession
import org.example.idDataTypeSparkExample.shared.Config

class Application(config: Config){
	private val sparkSession = SparkSession.builder().master(config.master).getOrCreate()

	def run(): Unit = {
		val dataStore = new PrepareData(sparkSession)
		if (config.buildData) {
			dataStore
				.writeDFs(
					config.workDirectory,
					dataStore
						.buildSource(config.rangeStartId, config.rangeEndId, config.rangeStep)
						.orderBy("id")
				)
		}
		if (config.buildJoins) {
			new JoinTest(sparkSession).runJoins(config.workDirectory).show()
		}

		if(config.waitForUser){
			import scala.io.StdIn.readLine
			readLine()
		}
	}
}