package org.example.idDataTypeSparkExample.shared

object ConfigKeys {
  val workDirectory = "workDirectory"
  val fileFormats="fileFormats"
  val analyzeReferenceCompression="analyzeReferenceCompression"
  val analyzeReferenceFileFormat="analyzeReferenceFileFormat"
  val analyzeReferenceDataType="analyzeReferenceDataType"
  val buildData = "buildData"
  val buildExplain = "buildExplain"
  val buildRangeStartId = "buildRangeStartId"
  val buildRangeEndId = "buildRangeEndId"
  val buildRangeStep = "buildRangeStep"
  val buildRepartition = "buildRepartition"
  val buildCompressions = "buildCompressions"
  val buildSingleIdColumn = "buildSingleIdColumn"
  val buildSliceCount = "buildSliceCount"
  val buildWriteThreadCount= "buildWriteThreadCount"
  val testJoins = "testJoins"
  val testJoinsExplain = "testJoinsExplain"
  val waitForUser = "waitForUser"
  val logStatDir = "logStatDir"

  val params = Seq(
    workDirectory,
    fileFormats,
    analyzeReferenceCompression,
    analyzeReferenceFileFormat,
    analyzeReferenceDataType,
    buildCompressions,
    buildData,
    buildExplain,
    buildRangeStartId,
    buildRangeEndId,
    buildRangeStep,
    buildRepartition,
    buildSingleIdColumn,
    buildSliceCount,
    buildWriteThreadCount,
    testJoins,
    testJoinsExplain,
    waitForUser,
    logStatDir)
}
