package org.example.idDataTypeSparkExample

object ConfigKeys extends Enumeration {
  val master = "master"
  val rangeStartId = "rangeStartId"
  val rangeEndId = "rangeEndId"
  val rangeStep = "rangeStep"
  val workDirectory = "workDirectory"
  val buildData = "buildData"
  val buildJoins = "buildJoins"
}