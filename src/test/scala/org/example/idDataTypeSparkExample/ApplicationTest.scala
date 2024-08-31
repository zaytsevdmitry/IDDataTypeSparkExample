package org.example.idDataTypeSparkExample

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.example.idDataTypeSparkExample.shared.{Config, Constants, FileSystemStructure, TypePath}

class ApplicationTest extends Test {

	val writeFSTestDir = "tmp/org.example.idDataTypeSparkExample.test/WriteDfs"
	val writeFSTestDirPath = new Path(s"$writeFSTestDir")
  val sourceTemplatePath = s"$writeFSTestDir/sourcetemplate"
  val testTypePaths: Seq[TypePath] = new FileSystemStructure(writeFSTestDir,Array("parquet","orc", "csv"),Array("gzip","none")).typePaths
  val testingSetRowCount = 320000
  def buildSource():DataFrame ={

    new BuildData(sparkSession)
      .buildSource(
        startId=1L,
        endId=testingSetRowCount,
        step=1L,
        repartitionWrite = 10,
        sourceTemplatePath=sourceTemplatePath,
        buildExplain = true,
        buildSliceCount = 100000
      )
    sparkSession.read.parquet(sourceTemplatePath)
  }



  "buildSource" should "work" in {
    cleanDFs()
    val s = buildSource()
    s.printSchema()
    s.show(1000)
    assert(s.count() == testingSetRowCount)
  }

  "pathTypePairList" should "work" in {
    val fssSeq = testTypePaths

    val pathLeft = fssSeq.head.pathLeft

    assert(pathLeft.startsWith(writeFSTestDir))
    fssSeq.foreach(p=>{
      println(p.fileFormat)
      println(p.compression)
      println(p.logicalTypeName)
      println(p.pathLeft)
      println(p.pathRight)
      println("------------------")
    })
  }

  def cleanDFs(): Unit = {

    println(s"cleanup $writeFSTestDir")
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    if (fs.exists(writeFSTestDirPath))
      fs.delete(writeFSTestDirPath, true)
  }

  def writeDFsSingleColumn(buildSingle:Boolean): Unit = {
    val ds = new BuildData(sparkSession)
    ds.writeDFs(sourceDataFrame=buildSource(),
      repartitionWrite=2,
      buildSingleIdColumn=buildSingle,
      writeThreadCount=2,
      typePaths = testTypePaths
    )
  }

  "Write testing sets" should "work" in {
    cleanDFs()
    writeDFsSingleColumn( true)

    testTypePaths
     .filter(v => v.fileFormat.equals("parquet") && v.compression.equals("gzip"))// test only works types
      .foreach(v => {
        assert(sparkSession.read.parquet(v.pathLeft).count() == testingSetRowCount)
        assert(sparkSession.read.parquet(v.pathRight).count() == testingSetRowCount)
    })

  }
  "runJoins testing sets" should "work" in {
    cleanDFs()
    writeDFsSingleColumn(true)
    new JoinTest(sparkSession).runJoins(testTypePaths).show()
  }



  "getSizeMB" should "work" in {
    cleanDFs()
    writeDFsSingleColumn(true)
    new BuildData(sparkSession).statSize(testTypePaths).show()
  }

  "Analyze stat" should "work" in {
    cleanDFs()
    writeDFsSingleColumn(true)
    val analyzeStat = new AnalyzeStat().analyzeStat(
      new BuildData(sparkSession).statSize(testTypePaths),
      new JoinTest(sparkSession).runJoins(testTypePaths),
      referenceFileFormat = "parquet",
      referenceCompression = "gzip",
      referenceDataType = "bigint"
    )
    analyzeStat.printSchema()
    analyzeStat.show()
    assert(
      analyzeStat.count() == 16
    )
  }

  "Config validation" should "wrong" in {
    val cfg = Config(
      workDirectory = "",
      fileFormats = Array("parquet","csv"),
      analyzeReferenceCompression = "zstd",
      analyzeReferenceFileFormat = "orc",
      analyzeReferenceDataType = "bigint",
      buildCompressions = Array("gzip", "none"),
      buildData = false,
      buildExplain = false,
      buildRangeStartId = 1,
      buildRangeEndId = 1,
      buildRangeStep = 1,
      buildRepartition = 1,
      buildSingleIdColumn = false,
      buildSliceCount = 1,
      buildWriteThreadCount = 1,
      testJoins = false,
      testJoinsExplain = false,
      waitForUser = false,
      logStatDir = "")
    assert(!Main.validateConfig(cfg))
  }
  "Config validation" should "right" in {
    val cfg = Config(
      workDirectory = "",
      fileFormats = Array("parquet","csv"),
      analyzeReferenceCompression = "gzip",
      analyzeReferenceFileFormat = "parquet",
      analyzeReferenceDataType = "bigint",
      buildCompressions = Array("gzip", "none"),
      buildData = false,
      buildExplain = false,
      buildRangeStartId = 1,
      buildRangeEndId = 1,
      buildRangeStep = 1,
      buildRepartition = 1,
      buildSingleIdColumn = false,
      buildSliceCount = 1,
      buildWriteThreadCount = 1,
      testJoins = false,
      testJoinsExplain = false,
      waitForUser = false,
      logStatDir = "")
    assert(Main.validateConfig(cfg))
  }
}

