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
      rangeStartId = cfg(ConfigKeys.rangeStartId).toLong,
      rangeEndId = cfg(ConfigKeys.rangeEndId).toLong,
      rangeStep = cfg(ConfigKeys.rangeStep).toLong,
      workDirectory = cfg(ConfigKeys.workDirectory),
      buildData = cfg(ConfigKeys.buildData).toBoolean,
      buildCached = cfg(ConfigKeys.buildCached).toBoolean,
      buildRepartition = cfg(ConfigKeys.buildRepartition).toInt,
      buildCompression = cfg(ConfigKeys.buildCompression),
      buildJoins = cfg(ConfigKeys.testJoins).toBoolean,
      waitForUser = cfg(ConfigKeys.waitForUser).toBoolean,
    )

    new Application(config).run()

  }
}