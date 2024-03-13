package org.example.idDataTypeSparkExample

object Main {

  def main(args: Array[String]): Unit = {

    val cfg = args.map(v => v.split("=")).map(v => v(0) -> v(1)).toMap

    cfg.foreach(println)
    val config = Config(
      cfg(ConfigKeys.master),
      cfg(ConfigKeys.rangeStartId).toLong,
      cfg(ConfigKeys.rangeEndId).toLong,
      cfg(ConfigKeys.rangeStep).toLong,
      cfg(ConfigKeys.workDirectory),
      cfg(ConfigKeys.buildData).toBoolean,
      cfg(ConfigKeys.buildJoins).toBoolean,
    )

    new Application(config).run()

  }
}