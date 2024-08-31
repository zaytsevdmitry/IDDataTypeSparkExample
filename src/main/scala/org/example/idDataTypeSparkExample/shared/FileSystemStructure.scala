package org.example.idDataTypeSparkExample.shared

class FileSystemStructure (
  workDirectory: String,
  fileFormats:Array[String],
  compressions: Array[String]) {

  val typePaths:Seq[TypePath] = fileFormats.flatMap(fileFormat=>
    compressions.flatMap(compression =>
      Constants.types.map(logicalTypeName =>
        TypePath(
          s"$workDirectory/$fileFormat/$compression/$logicalTypeName/left",
          s"$workDirectory/$fileFormat/$compression/$logicalTypeName/right",
          fileFormat,
          compression,
          logicalTypeName))))


}
