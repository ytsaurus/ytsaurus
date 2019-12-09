package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.utils.YtTableUtils
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.io.Source

class YtFileSystemTest extends FlatSpec with Matchers with TmpDir {

  behavior of "YtFileSystemTest"

  private val conf = {
    val c = new Configuration()
    c.set("yt.proxy", "localhost:8000")
    c.set("yt.user", "root")
    c.set("yt.token", "")
    c
  }
  private val fs = new YtFileSystem
  fs.initialize(new Path("/").toUri, conf)
  fs.setConf(conf)

  override def yt: YtClient = fs.yt

  it should "listStatus" in {
    YtTableUtils.createDir(tmpPath)
    YtTableUtils.createDir(s"$tmpPath/1")
    YtTableUtils.createDir(s"$tmpPath/2")
    YtTableUtils.createFile(s"$tmpPath/3")
    YtTableUtils.writeBytesToFile(s"$tmpPath/3", "123".getBytes())

    val res = fs.listStatus(new Path(tmpPath)).map(f => (f.getPath, f.isDirectory, f.getLen))

    res should contain theSameElementsAs Seq(
      (new Path(s"$tmpPath/1"), true, 0),
      (new Path(s"$tmpPath/2"), true, 0),
      (new Path(s"$tmpPath/3"), false, 3)
    )
  }

  it should "open" in {
    YtTableUtils.createFile(s"$tmpPath")
    YtTableUtils.writeBytesToFile(s"$tmpPath", ("1" * 1024 * 1024).getBytes())

    val in = fs.open(new Path(tmpPath))
    try {
      val source = Source.fromInputStream(in)
      try {
        val res = source.mkString
        res shouldEqual "1" * 1024 * 1024
      } finally source.close()
    } finally in.close()
  }

  it should "create" in {
    val out = fs.create(new Path(tmpPath))
    try {
      out.write("123".getBytes())
    } finally {
      out.close()
    }

    val res = YtTableUtils.readFileString(tmpPath)
    res shouldEqual "123"
  }

  it should "rename" in {
    YtTableUtils.createDir(tmpPath)
    YtTableUtils.createFile(s"$tmpPath/1")
    YtTableUtils.writeBytesToFile(s"$tmpPath/1", "123".getBytes())

    fs.rename(new Path(s"$tmpPath/1"), new Path(s"$tmpPath/2"))

    YtTableUtils.exists(s"$tmpPath/1") shouldEqual false
    YtTableUtils.exists(s"$tmpPath/2") shouldEqual true
    YtTableUtils.readFileString(s"$tmpPath/2") shouldEqual "123"
  }

  it should "delete" in {
    YtTableUtils.createDir(tmpPath)

    fs.delete(new Path(tmpPath), recursive = false)

    YtTableUtils.exists(tmpPath) shouldEqual false
  }

}
