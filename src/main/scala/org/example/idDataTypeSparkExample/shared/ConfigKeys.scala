package org.example.idDataTypeSparkExample.shared

object ConfigKeys {
  val workDirectory = "workDirectory"
  val fileFormat="fileFormat"
  val buildData = "buildData"
  val buildRangeStartId = "buildRangeStartId"
  val buildRangeEndId = "buildRangeEndId"
  val buildRangeStep = "buildRangeStep"
  val buildCached = "buildCached"
  val buildRepartition = "buildRepartition"
  val buildCompression = "buildCompression"
  val buildExplain = "buildExplain"
  val buildSingleIdColumn = "buildSingleIdColumn"
  val testJoins = "testJoins"
  val testJoinsExplain = "testJoinsExplain"
  val waitForUser = "waitForUser"
  val logStatDir = "logStatDir"

  val params = Seq(
    workDirectory,
    fileFormat,
    buildData,
    buildRangeStartId,
    buildRangeEndId,
    buildRangeStep,
    buildCached,
    buildRepartition,
    buildCompression,
    buildExplain,
    buildSingleIdColumn,
    testJoins,
    testJoinsExplain,
    waitForUser,
    logStatDir)
}
