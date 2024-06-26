package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.example.idDataTypeSparkExample.shared.Constants

class ApplicationTest extends Test {

	val writeFstestDir = "tmp/org.example.idDataTypeSparkExample.test/WriteDfs"
	val writeFstestDirPath = new Path(s"$writeFstestDir")

  "buildSource" should "work" in {

    val s = new BuildData(sparkSession)
      .buildSource(startId=1L, endId=20L, step=1L, cached=true,repartitionWrite = 1)
    s.printSchema()
    s.show(1000)
    assert(s.count() == 20)
  }

  "pathTypePairList" should "work" in {
    val pathLeft = Constants.pathTypePairList(workDirectory = writeFstestDir).head._2.pathLeft
    assert(pathLeft.startsWith(writeFstestDir))
    println(Constants.pathTypePairList(workDirectory = writeFstestDir))
  }

  def cleanDFs(): Unit = {

    println(writeFstestDir)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    if (fs.exists(writeFstestDirPath))
      fs.delete(writeFstestDirPath, true)
  }

  def witeDFsSingleColumn(fileFormat:String, buildSingle:Boolean): Unit = {
    val ds = new BuildData(sparkSession)
    ds.writeDFs(
      writeFstestDir,
      ds.buildSource(
        startId=1L,
        endId=20L,
        step=1L,
        cached=true,
        repartitionWrite = 1),
      1,
      "none",
      fileFormat,
      buildSingle)
  }

  "Write parquet" should "work" in {
    cleanDFs()
    witeDFsSingleColumn("parquet", true)
    Constants.pathTypePairList(writeFstestDir).foreach(v => {
      assert(sparkSession.read.parquet(v._2.pathLeft).count() == 20)
      assert(sparkSession.read.parquet(v._2.pathRight).count() == 20)
    })

  }
  "runJoins parquet" should "work" in {
    cleanDFs()
    witeDFsSingleColumn("parquet",true)
    new JoinTest(sparkSession).runJoins(writeFstestDir,"parquet").show()
  }

  "runJoins orc" should "work" in {
    cleanDFs()
    witeDFsSingleColumn("orc",true)
    new JoinTest(sparkSession).runJoins(writeFstestDir,"orc").show()
  }


  "getSizeMB" should "work" in {
    cleanDFs()
    witeDFsSingleColumn("parquet",true)
    new BuildData(sparkSession).statSize(workDirectory = writeFstestDir).show()
  }

  "Analyze stat" should "work" in {
    cleanDFs()
    witeDFsSingleColumn("parquet",true)
    val analyzeStat = new AnalyzeStat(sparkSession).analyzeStat(
      new BuildData(sparkSession).statSize(writeFstestDir),
      new JoinTest(sparkSession).runJoins(writeFstestDir,"parquet"),
      "parquet",
      "none"
    )
    analyzeStat.printSchema()
    analyzeStat.show()
    assert(
      analyzeStat.count() == 4
    )
  }

  "tmp" should "work" in {
    val l = 8301034833169298228L / 2147483632
    println(l)
  }
}

