package org.example.idDataTypeSparkExample

import org.example.idDataTypeSparkExample.shared.{Config, ConfigKeys, Constants}

object Main {
  def buildConfig(args: Array[String]):Config = {
    ConfigKeys.params.foreach(v => println(s"$v="))
    args.foreach(a => println(a))
    val cfg = args
      .filter(v => v.contains("="))
      .map(v => v.split("="))
      .map(v => v(0) -> v(1))
      .toMap

    cfg.foreach(println)
    Config(
      workDirectory = cfg(ConfigKeys.workDirectory),
      fileFormats = cfg(ConfigKeys.fileFormats).split(","),

      buildData = cfg(ConfigKeys.buildData).toBoolean,
      buildExplain = cfg(ConfigKeys.buildExplain).toBoolean,
      analyzeReferenceCompression = cfg(ConfigKeys.analyzeReferenceCompression),
      analyzeReferenceFileFormat = cfg(ConfigKeys.analyzeReferenceFileFormat),
      analyzeReferenceDataType = cfg(ConfigKeys.analyzeReferenceDataType),
      buildRangeStartId = cfg(ConfigKeys.buildRangeStartId).toLong,
      buildRangeEndId = cfg(ConfigKeys.buildRangeEndId).toLong,
      buildRangeStep = cfg(ConfigKeys.buildRangeStep).toInt,
      buildRepartition = cfg(ConfigKeys.buildRepartition).toInt,
      buildCompressions = cfg(ConfigKeys.buildCompressions).split(","),
      buildSingleIdColumn = cfg(ConfigKeys.buildSingleIdColumn).toBoolean,
      buildSliceCount = cfg(ConfigKeys.buildSliceCount).toInt,
      buildWriteThreadCount = cfg(ConfigKeys.buildWriteThreadCount).toInt,
      testJoins = cfg(ConfigKeys.testJoins).toBoolean,
      testJoinsExplain = cfg(ConfigKeys.testJoinsExplain).toBoolean,
      waitForUser = cfg(ConfigKeys.waitForUser).toBoolean,
      logStatDir = cfg(ConfigKeys.logStatDir)
    )

  }
  def validateConfig(config: Config): Boolean ={
    def riseText(key: String, value: String, values:Seq[String]):String =
    {
      s"The value for the argument $key:$value is invalid. It could be one of the ${values.mkString(",")}"
    }
    val compressionStatus = config.buildCompressions.contains(config.analyzeReferenceCompression)
    val fileFormatStatus = config.fileFormats.contains(config.analyzeReferenceFileFormat)
    val dataTypesStatus = Constants.types.contains(config.analyzeReferenceDataType)

    if (!compressionStatus)
      println(riseText(ConfigKeys.analyzeReferenceCompression,config.analyzeReferenceCompression,config.buildCompressions))

    if (!fileFormatStatus)
      println(riseText(ConfigKeys.analyzeReferenceFileFormat,config.analyzeReferenceFileFormat,config.fileFormats))

    if (!dataTypesStatus)
      println(riseText(ConfigKeys.analyzeReferenceDataType,config.analyzeReferenceDataType,Constants.types))

    compressionStatus && fileFormatStatus && dataTypesStatus
  }

  def main(args: Array[String]): Unit = {

    val config = buildConfig(args)

    if(validateConfig(config))
      new Application(config).run()
  }
}