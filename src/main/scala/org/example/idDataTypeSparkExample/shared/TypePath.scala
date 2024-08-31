package org.example.idDataTypeSparkExample.shared

case class TypePath(
  pathLeft: String,
  pathRight: String,
  fileFormat:String,
  compression:String,
  logicalTypeName:String
)
