package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathNotFoundException}
import org.apache.hadoop.io.IOUtils
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.{LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper

import java.io.ByteArrayInputStream
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class YtFileSystemTest extends FlatSpec with Matchers with LocalYtClient with TmpDir {

  behavior of "YtFileSystemTest"

  override def testDir: String = "/tmp/test" // should start with single slash

  private val fsConf = {
    val c = new Configuration()
    c.set("yt.proxy", "localhost:8000")
    c.set("yt.user", "root")
    c.set("yt.token", "")
    c
  }
  private val fs = new YtFileSystem
  fs.initialize(new Path("/").toUri, fsConf)
  fs.setConf(fsConf)

  def writeBytesToFile(path: String, content: Array[Byte], timeout: Duration = 1 minute): Unit = {
    val os = YtWrapper.writeFile(path, timeout, transaction = None)
    try os.write(content) finally os.close()
  }

  it should "listStatus" in {
    YtWrapper.createDir(tmpPath)
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

  it should "throw an exception in listStatus of wrong path" in {
    val paths = List("//home/user/folder", "/slash").map(new Path(_))

    paths.foreach {
      path =>
        a[PathNotFoundException] shouldBe thrownBy {
          fs.listStatus(path)
        }
    }
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

  it should "overwrite existed file" in {
    var out = fs.create(new Path(tmpPath))
    try {
      out.write("123".getBytes())
    } finally {
      out.close()
    }

    out = fs.create(new Path(tmpPath), true)
    try {
      out.write("345".getBytes())
    } finally {
      out.close()
    }

    val res = YtWrapper.readFileAsString(tmpPath)
    res shouldEqual "345"
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

  it should "return 'false' when deleted path isn't exist" in {
    YtWrapper.createDir(tmpPath)

    val result = fs.delete(new Path(s"$tmpPath/file_is_not_exist"), false)

    result shouldEqual false
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

  it should "write file to fs" in {
    val dest = new Path(s"yt://$tmpPath")
    val text = ("1" * 1024 * 1024 * 10).getBytes

    val in = new ByteArrayInputStream(text)
    try {
      IOUtils.copyBytes(in, fs.create(dest), 1048576, true)
    } finally {
      in.close()
    }

    YtWrapper.exists(tmpPath) shouldEqual true
    YtWrapper.fileSize(tmpPath, None) shouldEqual text.length
  }
}
