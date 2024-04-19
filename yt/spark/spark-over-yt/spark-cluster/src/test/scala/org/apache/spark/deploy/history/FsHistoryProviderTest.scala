package org.apache.spark.deploy.history

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.fs.YtFileSystem
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.io.FileNotFoundException

class FsHistoryProviderTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir {
  behavior of "FsHistoryProvider"

  it should "create a directory for logs if it doesn't exist and config is set to true" in {
    runTest(flag = true, createDir = false, expectedMkdirsCounter = 1)
  }

  it should "not create a directory for logs if it already exists" in {
    runTest(flag = true, createDir = true, expectedMkdirsCounter = 0)
  }

  it should "throw an exception if a directory doesn't exist and config is set to false" in {
    val exception = intercept[FileNotFoundException](
      runTest(flag = false, createDir = false, expectedMkdirsCounter = 0)
    )

    exception.getMessage shouldEqual s"Log directory specified does not exist: yt:/$tmpPath/logs"
  }

  private def runTest(flag: Boolean, createDir: Boolean, expectedMkdirsCounter: Int): Unit = {
    val logsPath = s"$tmpPath/logs"
    val logsPathUrl = s"yt:/$logsPath"

    val conf = sparkConf
    conf.set("fs.yt.impl", classOf[YtFileSystemSpy].getName)
    conf.set("spark.hadoop.fs.yt.impl", classOf[YtFileSystemSpy].getName)
    conf.set("spark.history.fs.createLogDirectory", flag.toString)
    conf.set("spark.history.fs.logDirectory", logsPathUrl)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

    val ytFileSystemSpy = new Path(logsPathUrl).getFileSystem(hadoopConf).asInstanceOf[YtFileSystemSpy]
    ytFileSystemSpy.mkdirsCounter = 0

    if (createDir) {
      YtWrapper.createDir(logsPath)
    } else {
      YtWrapper.exists(logsPath) shouldBe false
    }
    new FsHistoryProvider(conf).initialize()

    YtWrapper.exists(logsPath) shouldBe true
    ytFileSystemSpy.mkdirsCounter shouldBe expectedMkdirsCounter
  }
}

class YtFileSystemSpy extends YtFileSystem {
  var mkdirsCounter = 0

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    mkdirsCounter += 1
    super.mkdirs(f, permission)
  }
}