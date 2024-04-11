package org.example.idDataTypeSparkExample.shared

case class Config(
  workDirectory: String,
  fileFormat:String,
  buildData: Boolean,
  buildRangeStartId:Long,
  buildRangeEndId:Long,
  buildRangeStep:Int,
  buildCached: Boolean,
  buildRepartition: Int,
  buildCompression: String,
  buildExplain:Boolean,
  buildSingleIdColumn:Boolean,
  testJoins: Boolean,
  testJoinsExplain: Boolean,
  waitForUser: Boolean,
  logStatDir:String
)
