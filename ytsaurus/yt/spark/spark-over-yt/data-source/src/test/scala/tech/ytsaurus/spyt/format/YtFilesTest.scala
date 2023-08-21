package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.duration._
import scala.language.postfixOps

class YtFilesTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtDataSource"

  import spark.implicits._

  it should "write parquet files to yt" in {
    Seq(1, 2, 3).toDF.write.parquet(s"yt:/$tmpPath")

    YtWrapper.isDir(tmpPath) shouldEqual true
    val files = YtWrapper.listDir(tmpPath)
    files.length shouldEqual 3
    files.foreach(name => name.endsWith(".parquet") shouldEqual true)

    val tmpLocalDir = Files.createTempDirectory("test_parquet")
    files.par.foreach { name =>
      val localPath = new File(tmpLocalDir.toFile, name).getPath
      YtWrapper.downloadFile(s"$tmpPath/$name", localPath)
    }
    spark.read.parquet(s"file://$tmpLocalDir").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  // Ignored while YtFsInputStream's backward `seek` method is not implemented
  it should "read parquet files from yt" ignore {
    Seq(1, 2, 3).toDF.write.parquet(s"yt:/$tmpPath")

    spark.read.parquet(s"yt:/$tmpPath").as[Int].collect() should contain theSameElementsAs Seq(1, 2, 3)
  }

  it should "read csv" in {
    YtWrapper.createFile(tmpPath)
    val os = YtWrapper.writeFile(tmpPath, 1 minute, None)
    try {
      os.write(
        """a,b,c
          |1,2,3
          |4,5,6""".stripMargin.getBytes(StandardCharsets.UTF_8))
    } finally os.close()

    val res = spark.read.option("header", "true").csv(tmpPath.drop(1))

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row("1", "2", "3"),
      Row("4", "5", "6")
    )
  }

  it should "read large csv" ignore {
    writeFileFromResource("test.csv", tmpPath)
    spark.read.csv(s"yt:/$tmpPath").count() shouldEqual 100000
  }
}
