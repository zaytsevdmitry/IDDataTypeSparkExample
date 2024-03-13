package org.example.idDataTypeSparkExample

case class Config(
                   master: String,
                   rangeStartId: Long,
                   rangeEndId: Long,
                   rangeStep: Long,
                   workDirectory: String,
                   buildData: Boolean,
                   buildJoins: Boolean
                 )
