package org.example.idDataTypeSparkExample.shared

object Constants{

	val _bigint = "bigint"
	val _string = "string"
	val _decimal = "decimal"

	val types = Seq(_bigint, _string, _decimal)
	val pathTypePairList = types.map(t => t -> TypePath(s"left/$t", s"right/$t"))

	case class TypePath(pathLeft: String, pathRight: String)

}
