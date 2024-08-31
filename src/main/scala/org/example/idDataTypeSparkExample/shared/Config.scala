package org.example.idDataTypeSparkExample.shared

case class Config(
  workDirectory: String,
  fileFormats:Array[String],
  analyzeReferenceCompression: String,
  analyzeReferenceFileFormat: String,
  analyzeReferenceDataType: String,
  buildCompressions: Array[String],
  buildData: Boolean,
  buildExplain: Boolean,
  buildRangeStartId:Long,
  buildRangeEndId:Long,
  buildRangeStep:Int,
  buildRepartition: Int,
  buildSingleIdColumn:Boolean,
  buildSliceCount:Int,
  buildWriteThreadCount:Int,
  testJoins: Boolean,
  testJoinsExplain: Boolean,
  waitForUser: Boolean,
  logStatDir:String
)
