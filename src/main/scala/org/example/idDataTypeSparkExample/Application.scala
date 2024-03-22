package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession
import org.example.idDataTypeSparkExample.shared.{Columns, Config}

import scala.io.StdIn.readLine

class Application(config: Config) {
  private val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  def run(): Unit = {
    val dataStore = new PrepareData(sparkSession)

    if (config.buildData) {
      dataStore
        .writeDFs(
          config.workDirectory,
          dataStore
            .buildSource(config.rangeStartId, config.rangeEndId, config.rangeStep, config.buildCached),
          config.buildRepartition,
          config.buildCompression
        )
    }
    val dataStatDf = dataStore.statSize(config.workDirectory).orderBy(Columns._type_name).cache()
    dataStatDf.show()

    if (config.buildJoins) {
      val jointestDf = new JoinTest(sparkSession)
				.runJoins(config.workDirectory)
				.orderBy(Columns._type_name).cache()

			jointestDf.show()

			new AnalyzeStat(sparkSession)
				.analyzeStat(dataStatDf, jointestDf)
				.orderBy(Columns._type_name)
				.show()
    }

    if (config.waitForUser) {
      waitForUserExit()
    }
  }

  def waitForUserExit(): Unit = {
    println("please, type quit to exit)")
    if (!"quit".equals(readLine())) waitForUserExit()
  }

}