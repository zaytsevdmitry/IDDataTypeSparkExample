package org.example.idDataTypeSparkExample.shared

case class Config(
  rangeStartId: Long,
  rangeEndId: Long,
  rangeStep: Long,
  workDirectory: String,
  buildData: Boolean,
  buildCached: Boolean,
  buildRepartition: Int,
  buildCompression: String,
  buildJoins: Boolean,
  waitForUser: Boolean
)
