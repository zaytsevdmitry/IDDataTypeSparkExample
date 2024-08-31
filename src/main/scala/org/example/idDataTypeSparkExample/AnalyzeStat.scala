package org.example.idDataTypeSparkExample

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.column
import org.example.idDataTypeSparkExample.shared.Columns

class AnalyzeStat() {

  private def dfToTable(dataFrame: DataFrame,
                               referenceCompression: String,
                               referenceFileFormat: String,
                               referenceDataType: String,
                               columnName: String):DataFrame = {
    import org.apache.spark.sql.functions.{round, when}
    dataFrame
      .filter(s"${Columns._type_name} = '$referenceDataType'")
      .filter(s"${Columns._file_format} = '$referenceFileFormat'")
      .filter(s"${Columns._file_compression} = '$referenceCompression'")
      .select(column(columnName).as(s"${columnName}_target"))
      .crossJoin(dataFrame)
      .withColumn(
        s"${columnName}_pcnt",
        round(
          when(column(s"${columnName}_target")===0,0)
            .otherwise(when(column(columnName) ===0, 0)
              .otherwise(column(columnName)/column(s"${columnName}_target")*100),
            ),2))

  }

  def analyzeStat(dataStatDF: DataFrame,
                  joinTestDF: DataFrame,
                  referenceCompression: String,
                  referenceFileFormat: String,
                  referenceDataType: String
                 ): DataFrame = {

    val dfSizes = dfToTable(dataStatDF,referenceCompression,referenceFileFormat,referenceDataType,Columns._size_mb)
    val dfJoinTimes = dfToTable(joinTestDF,referenceCompression,referenceFileFormat,referenceDataType,Columns._duration_s)

    val selectColumns = Seq(
      Columns._log_dt,
      Columns._type_name,
      Columns._file_format,
      Columns._file_compression,
      Columns._size_mb,
      Columns._size_mb_pcnt,
      Columns._duration_s,
      Columns._duration_s_pcnt,
      Columns._size_status,
      Columns._join_status
    )
    val keyColumns = Seq(
      Columns._type_name,
      Columns._file_format,
      Columns._file_compression
    )

    dfSizes
      .join(
        dfJoinTimes,
        keyColumns
          .toArray.clone(),
      "full")
      .select(selectColumns.map(column): _*)
      .sort(keyColumns.map(column): _*)

  }
}
