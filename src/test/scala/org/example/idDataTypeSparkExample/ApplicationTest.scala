package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.example.idDataTypeSparkExample.shared.Constants

class ApplicationTest extends Test{
	val testDir = "/tmp/org.example.idDataTypeSparkExample.test"

	"buildSource" should "work" in {

		val s = new PrepareData(sparkSession).buildSource(1L, 20L, 1L)
		s.show(1000)
		assert(s.count() == 20)
	}

	"pathTypePairList" should "work" in {
		println(Constants.pathTypePairList)
	}

	val writeFstestDir = s"$testDir/WriteDfs"
	val writeFstestDirPath = new Path(s"$writeFstestDir")

	def cleanDFs(): Unit = {

		println(writeFstestDir)
		val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
		if (fs.exists(writeFstestDirPath))
			fs.delete(writeFstestDirPath, true)

	}

	def witeDFs(): Unit = {
		val ds = new PrepareData(sparkSession)
		ds.writeDFs(s"$writeFstestDir", ds.buildSource(1L, 20L, 1L))

	}

	"WriteDfs" should "work" in {
		cleanDFs()
		witeDFs()
		Constants.pathTypePairList.foreach(v => {
			assert(sparkSession.read.parquet(s"$writeFstestDir/${v._2.pathLeft}").count() == 20)
			assert(sparkSession.read.parquet(s"$writeFstestDir/${v._2.pathRight}").count() == 20)
		})

	}
	"runJoins" should "work" in {
		cleanDFs()
		witeDFs()
		new JoinTest(sparkSession).runJoins(writeFstestDir).show()
	}

	"tmp" should "work" in {
		val l = 8301034833169298228L / 2147483632
		println(l)
	}
}
