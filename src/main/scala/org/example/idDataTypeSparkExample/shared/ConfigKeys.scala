package org.example.idDataTypeSparkExample.shared

object ConfigKeys {
  val rangeStartId = "rangeStartId"
  val rangeEndId = "rangeEndId"
  val rangeStep = "rangeStep"
  val workDirectory = "workDirectory"
  val buildData = "buildData"
  val buildCached = "buildCached"
  val buildRepartition = "buildRepartition"
  val buildCompression = "buildCompression"
  val testJoins = "testJoins"
  val waitForUser = "waitForUser"
  val params = Seq(
    rangeStartId,
    rangeEndId,
    rangeStep,
    workDirectory,
    buildData,
    buildCached,
    buildRepartition,
    buildCompression,
    testJoins,
    waitForUser)
}