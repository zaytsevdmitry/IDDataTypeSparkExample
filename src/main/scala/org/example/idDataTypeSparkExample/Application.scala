package org.example.idDataTypeSparkExample

import org.apache.spark.sql.SparkSession

//import org.example.idDataTypeSparkExample.SourceData
class Application(config: Config) {
  val sparkSession = SparkSession.builder().master(config.master).getOrCreate()
  val dataStore = new PrepareData(sparkSession)
  def run(): Unit = {

    if (config.buildData) {
      dataStore
        .writeDFs(
          config.workDirectory,
          dataStore
            .buildSource(config.rangeStartId, config.rangeEndId, config.rangeStep)
            .orderBy("id")
        )
    }
    if (config.buildJoins){
      new JoinTest(sparkSession).runJoins(config.workDirectory)
    }
  }
}