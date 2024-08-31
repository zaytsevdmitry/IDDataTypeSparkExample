package org.example.idDataTypeSparkExample

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.example.idDataTypeSparkExample.shared.{Columns, Config, FileSystemStructure}

import scala.io.StdIn.readLine

class Application(config: Config) {
  private val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  def run(): Unit = {

    val dataStore = new BuildData(sparkSession)
    val fileSystemStructure = new FileSystemStructure(
      workDirectory = config.workDirectory,
      fileFormats = config.fileFormats,
      compressions = config.buildCompressions)

    if (config.buildData) {
       val sourceTemplatePath = s"${config.workDirectory}/sourcetemplate"
       dataStore
        .buildSource(
          config.buildRangeStartId,
          config.buildRangeEndId,
          config.buildRangeStep,
          config.buildRepartition,
          sourceTemplatePath,
          config.buildExplain,
          config.buildSliceCount

        )

      val sourceDataFrame = sparkSession.read.parquet(sourceTemplatePath)

      dataStore
        .writeDFs(
          sourceDataFrame = sourceDataFrame,
          repartitionWrite = config.buildRepartition,
          buildSingleIdColumn = config.buildSingleIdColumn,
          writeThreadCount = config.buildWriteThreadCount,
          typePaths = fileSystemStructure.typePaths
        )
    }

    val dataStatDf = dataStore
      .statSize(fileSystemStructure.typePaths)
      .orderBy(Columns._type_name).cache()

    dataStatDf.show(1000,false)



    if (config.testJoins) {

      val jointestDf = new JoinTest(sparkSession)
        .runJoins(fileSystemStructure.typePaths)
        .orderBy(Columns._type_name).cache()

      val analyzeStatDf = new AnalyzeStat()
        .analyzeStat(
          dataStatDF = dataStatDf,
          joinTestDF = jointestDf,
          referenceCompression = config.analyzeReferenceCompression,
          referenceFileFormat = config.analyzeReferenceFileFormat,
          referenceDataType = config.analyzeReferenceDataType)
        .cache()


      analyzeStatDf.write.mode(SaveMode.Append).parquet(s"${config.logStatDir}/analyze_stat_df")

      analyzeStatDf.filter(s"${Columns._join_status} = 'error'").show(1000,false)
      analyzeStatDf.filter(s"${Columns._join_status} = 'ok'").show(1000,false)
    }

    if (config.waitForUser) {
      waitForUserExit()
    }
  }

  def waitForUserExit(): Unit = {
    println("please, type quit to exit)")
    if (!"quit".equals(readLine().trim)) waitForUserExit()
  }

}