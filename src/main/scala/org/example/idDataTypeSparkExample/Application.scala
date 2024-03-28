package org.example.idDataTypeSparkExample

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, Config, ConfigKeys}
import org.apache.spark.sql.functions.lit

import java.sql.Timestamp
import java.util.Date
import scala.io.StdIn.readLine

class Application(config: Config) {
  private val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  def run(): Unit = {

    val dataStore = new BuildData(sparkSession)

    if (config.buildData) {

      val sourceDataFrame = dataStore
        .buildSource(
          config.buildRangeStartId,
          config.buildRangeEndId,
          config.buildRangeStep,
          config.buildCached)

      if (config.buildExplain) sourceDataFrame.explain()

      dataStore
        .writeDFs(
          workDirectory = config.workDirectory,
          sourceDataFrame = sourceDataFrame,
          repartitionWrite = config.buildRepartition,
          compression = config.buildCompression,
          fileFormat = config.fileFormat
        )
    }
    val dataStatDf = dataStore.statSize(config.workDirectory).orderBy(Columns._type_name).cache()
    dataStatDf.show()

    if (config.testJoins) {
      val jointestDf = new JoinTest(sparkSession)
        .runJoins(config.workDirectory, config.fileFormat)
        .orderBy(Columns._type_name).cache()

      jointestDf.show()

      val analyzeStatDf = new AnalyzeStat(sparkSession)
        .analyzeStat(dataStatDf, jointestDf,config.fileFormat,config.buildCompression)
        .orderBy(Columns._type_name)

      analyzeStatDf.show()
      analyzeStatDf.write.mode(SaveMode.Append).parquet(s"${config.logStatDir}/analyze_stat_df")

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