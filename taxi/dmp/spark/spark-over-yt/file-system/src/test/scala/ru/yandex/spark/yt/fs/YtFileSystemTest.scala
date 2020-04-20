package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.TmpDir
import ru.yandex.spark.yt.wrapper.YtWrapper

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class YtFileSystemTest extends FlatSpec with Matchers with TmpDir {

  behavior of "YtFileSystemTest"

  override def testDir: String = "/tmp/test" // should start with single slash

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

  def writeBytesToFile(path: String, content: Array[Byte], timeout: Duration = 1 minute): Unit = {
    val os = YtWrapper.writeFile(path, timeout, transaction = None)
    try os.write(content) finally os.close()
  }

  it should "listStatus" in {
    println(tmpPath)
    YtWrapper.createDir(tmpPath)
    println("created")
    YtWrapper.createDir(s"$tmpPath/1")
    YtWrapper.createDir(s"$tmpPath/2")
    YtWrapper.createFile(s"$tmpPath/3")
    writeBytesToFile(s"$tmpPath/3", "123".getBytes())

    val res = fs.listStatus(new Path(tmpPath)).map(f => (f.getPath, f.isDirectory, f.getLen))

    res should contain theSameElementsAs Seq(
      (new Path(s"$tmpPath/1"), true, 0),
      (new Path(s"$tmpPath/2"), true, 0),
      (new Path(s"$tmpPath/3"), false, 3)
    )
  }

  it should "open" in {
    YtWrapper.createFile(s"$tmpPath")
    writeBytesToFile(s"$tmpPath", ("1" * 1024 * 1024).getBytes())

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

    val res = YtWrapper.readFileAsString(tmpPath)
    res shouldEqual "123"
  }

  it should "rename" in {
    YtWrapper.createDir(tmpPath)
    YtWrapper.createFile(s"$tmpPath/1")
    writeBytesToFile(s"$tmpPath/1", "123".getBytes())

    fs.rename(new Path(s"$tmpPath/1"), new Path(s"$tmpPath/2"))

    YtWrapper.exists(s"$tmpPath/1") shouldEqual false
    YtWrapper.exists(s"$tmpPath/2") shouldEqual true
    YtWrapper.readFileAsString(s"$tmpPath/2") shouldEqual "123"
  }

  it should "delete" in {
    YtWrapper.createDir(tmpPath)

    fs.delete(new Path(tmpPath), recursive = false)

    YtWrapper.exists(tmpPath) shouldEqual false
  }

  it should "consider timeout" ignore {
    val out = fs.create(new Path(tmpPath))
    try {
      Thread.sleep((150 seconds).toMillis)
      out.write("123".getBytes())
    } finally {
      out.close()
    }

    val res = YtWrapper.readFileAsString(tmpPath)
    res shouldEqual "123"
  }

}
