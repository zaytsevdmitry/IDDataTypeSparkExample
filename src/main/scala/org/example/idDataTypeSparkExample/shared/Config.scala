package org.example.idDataTypeSparkExample.shared

case class Config(
	master: String,
	rangeStartId: Long,
	rangeEndId: Long,
	rangeStep: Long,
	workDirectory: String,
	buildData: Boolean,
	buildJoins: Boolean,
	waitForUser:Boolean
)
