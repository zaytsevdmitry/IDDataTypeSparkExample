package org.example.idDataTypeSparkExample

object SharedVars {

  val _bigint = "bigint"
  val _string = "string"
  val _decimal = "decimal"

  val types = Seq(_bigint,_string,_decimal)
  val sides = Seq("left", "right")

  case class TypePath (pathLeft:String,pathRight:String)
  val pathTypePairList = types.map( t=> t->TypePath(s"left/$t",s"right/$t"))

}
