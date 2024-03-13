package org.example.idDataTypeSparkExample
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{column, lit, make_interval}
class ApplicationTest extends Test{
  val testDir="/tmp/org.example.idDataTypeSparkExample.test"

  "buildSource" should "work" in {

    val s = new PrepareData(sparkSession).buildSource(1L,20L,1L)
    s.show(1000)
    assert(s.count() == 20)
  }

  "pathTypePairList" should "work" in {
    println(SharedVars.pathTypePairList)
  }

  val writeFstestDir = s"$testDir/WriteDfs"
  val writeFstestDirPath = new Path(s"$writeFstestDir")

  def cleanDFs(): Unit = {

    println(writeFstestDir)
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    if(fs.exists(writeFstestDirPath))
      fs.delete(writeFstestDirPath,true)

  }
  def witeDFs(): Unit = {
    val ds = new PrepareData(sparkSession)
    ds.writeDFs(s"$writeFstestDir", ds.buildSource(3999999L,6999999L,1L))

  }
  "WriteDfs" should "work" in {
    cleanDFs()
    witeDFs()
    SharedVars.pathTypePairList.foreach(v=> {
      assert(sparkSession.read.parquet(s"$writeFstestDir/${v._2.pathLeft}").count() == 20)
      assert(sparkSession.read.parquet(s"$writeFstestDir/${v._2.pathRight}").count() == 20)
    })

  }
  "runJoins" should "work" in {
    cleanDFs()
    witeDFs()
    new JoinTest(sparkSession).runJoins(writeFstestDir).show()
  }
  "" should "SDAS" in {
    val df = sparkSession.createDataFrame(
      Seq(
        (1, "1600676073054")
      )
    ).toDF("id","long_timestamp")

    df.withColumn(
      "timestamp_mili",
      (column("long_timestamp")/1000).cast("timestamp")
    )
      .withColumn(
        "duration1",
        (make_interval(lit(0),lit(0),lit(0),lit(0),lit(0),lit(0),column("long_timestamp")/1000))
      )
      .show(false)
  }
}
