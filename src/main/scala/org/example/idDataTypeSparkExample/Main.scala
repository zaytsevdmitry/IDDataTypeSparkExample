package org.example.idDataTypeSparkExample

import org.example.idDataTypeSparkExample.shared.{Config, ConfigKeys}

object Main{

	def main(args: Array[String]): Unit = {
		ConfigKeys.params.foreach(v => println(s"$v="))
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
			cfg(ConfigKeys.waitForUser).toBoolean,
		)

		new Application(config).run()

	}
}