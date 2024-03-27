package org.example.idDataTypeSparkExample

import org.example.idDataTypeSparkExample.shared.{Config, ConfigKeys}

object Main {

  def main(args: Array[String]): Unit = {
    ConfigKeys.params.foreach(v => println(s"$v="))
    args.foreach(a => println(a))
    val cfg = args
      .filter(v => v.contains("="))
      .map(v => v.split("="))
      .map(v => v(0) -> v(1))
      .toMap

    cfg.foreach(println)

    val config = Config(
      workDirectory = cfg(ConfigKeys.workDirectory),
      fileFormat =  cfg(ConfigKeys.fileFormat),
      buildData = cfg(ConfigKeys.buildData).toBoolean,
      buildRangeStartId = cfg(ConfigKeys.buildRangeStartId).toLong,
      buildRangeEndId = cfg(ConfigKeys.buildRangeEndId).toLong,
      buildRangeStep = cfg(ConfigKeys.buildRangeStep).toInt,
      buildCached = cfg(ConfigKeys.buildCached).toBoolean,
      buildRepartition = cfg(ConfigKeys.buildRepartition).toInt,
      buildCompression = cfg(ConfigKeys.buildCompression),
      buildExplain = cfg(ConfigKeys.buildExplain).toBoolean,
      testJoins = cfg(ConfigKeys.testJoins).toBoolean,
      testJoinsExplain = cfg(ConfigKeys.testJoinsExplain).toBoolean,
      waitForUser = cfg(ConfigKeys.waitForUser).toBoolean,
      logStatDir = cfg(ConfigKeys.logStatDir)
    )

    new Application(config).run()

  }
}