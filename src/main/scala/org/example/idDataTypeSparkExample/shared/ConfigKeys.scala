package org.example.idDataTypeSparkExample.shared

object ConfigKeys{
	val master = "master"
	val rangeStartId = "rangeStartId"
	val rangeEndId = "rangeEndId"
	val rangeStep = "rangeStep"
	val workDirectory = "workDirectory"
	val buildData = "buildData"
	val buildJoins = "buildJoins"
	val waitForUser = "waitForUser"
	val params = Seq(master, rangeStartId, rangeEndId, rangeStep, workDirectory, buildData, buildJoins, waitForUser)
}