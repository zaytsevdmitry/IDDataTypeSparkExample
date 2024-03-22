package org.example.idDataTypeSparkExample.shared

object Constants {

  val _bigint = "bigint"
  val _string = "string"
  val _decimal = "decimal"
  val _uuid = "uuid"

  val types = Seq(_bigint, _string, _decimal, _uuid)

  def pathTypePairList(workDirectory: String):Seq[(String,TypePath)] =
    types.map(t => t -> TypePath(s"$workDirectory/left/$t", s"$workDirectory/right/$t"))

  case class TypePath(pathLeft: String, pathRight: String)

}
