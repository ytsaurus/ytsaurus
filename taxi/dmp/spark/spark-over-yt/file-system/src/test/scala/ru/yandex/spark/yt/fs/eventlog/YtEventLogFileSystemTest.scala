package ru.yandex.spark.yt.fs.eventlog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, Path}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.spark.yt.fs.PathUtils.{getMetaPath, hadoopPathToYt}
import ru.yandex.spark.yt.test.{LocalSpark, LocalYtClient, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._
import ru.yandex.spark.yt.wrapper.model.EventLogSchema._
import ru.yandex.yt.ytclient.tables.TableSchema

import java.io.FileNotFoundException
import java.time.{Clock, LocalDateTime, ZoneOffset}
import scala.io.Source
import scala.language.postfixOps

class YtEventLogFileSystemTest extends FlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils with TmpDir {
  behavior of "YtEventLogFileSystemTest"

  override def testDir: String = "/tmp/test" // should start with single slash

  def tableName = "table"

  def tableLocation = s"$tmpPath/$tableName"

  def metaTableLocation: String = getMetaPath(tableLocation)

  private val fsConf = {
    val c = new Configuration()
    c.set("yt.dynTable.rowSize", "2")
    c.set("fs.ytEventLog.singleReadLimit", "2")
    c.set("yt.proxy", "localhost:8000")
    c.set("yt.user", "root")
    c.set("yt.token", "")
    c
  }
  private val fs = new YtEventLogFileSystem
  fs.initialize(new Path("/").toUri, fsConf)
  fs.setClock(Clock.fixed(LocalDateTime.parse("2021-07-05T12:00:00").toInstant(ZoneOffset.UTC), ZoneOffset.UTC))
  fs.setConf(fsConf)


  private def getNameAndFileLocation(fileName: String): (String, String) = {
    (fileName, s"$tableLocation/$fileName")
  }

  it should "open" in {
    val (fileName, tablePath) = getNameAndFileLocation("testLog")

    YtWrapper.createDynTableAndMount(hadoopPathToYt(tableLocation), schema)
    YtWrapper.insertRows(hadoopPathToYt(tableLocation), schema, List(
      new YtEventLogBlock(fileName, 1, "12".getBytes).toList,
      new YtEventLogBlock(fileName, 2, "3".getBytes).toList,
    ), None)
    YtWrapper.createDynTableAndMount(hadoopPathToYt(metaTableLocation), metaSchema)
    YtWrapper.insertRows(hadoopPathToYt(metaTableLocation), metaSchema,
      List(
        new YtEventLogFileDetails(
          fileName, fileName, new YtEventLogFileMeta(2, 2, 3, System.currentTimeMillis())
        ).toList
      ), None)

    readAllData(tablePath) shouldEqual "123"
  }

  it should "throw an exception when reading not existing file" in {
    an[IllegalArgumentException] shouldBe thrownBy {
      fs.open(new Path(getNameAndFileLocation("unknownTestLog")._2)).close()
    }
  }

  // Gets id from meta table by filename
  private def getId(path: String, fileName: String): String = {
    fs.getFileDetailsImpl(hadoopPathToYt(path), fileName, None)
      .map(details => details.id)
      .getOrElse("undefined")
  }

  private def writeSingleStringToLog(path: String, s: String): Unit = {
    writeSingleStringToLog(new Path(path), s)
  }

  private def writeSingleStringToLog(path: Path, s: String): Unit = {
    val out = fs.create(path)
    try {
      out.write(s.getBytes())
    } finally {
      out.close()
    }
  }

  case class YtEventLogBlockTest(id: String,
                                 order: Long,
                                 log: List[Byte])

  object YtEventLogBlockTest {
    def apply(ytEventLogBlock: YtEventLogBlock): YtEventLogBlockTest = {
      YtEventLogBlockTest(ytEventLogBlock.id, ytEventLogBlock.order, ytEventLogBlock.log.toList)
    }
  }

  it should "create" in {
    val (fileName, tablePath) = getNameAndFileLocation("testLog")
    writeSingleStringToLog(tablePath, "123")
    val id = getId(tableLocation, fileName)

    val res = getAllRows(tableLocation, schema).map(x => YtEventLogBlockTest(YtEventLogBlock(x)))
    res should contain theSameElementsAs Seq(
      YtEventLogBlockTest(YtEventLogBlock(id, 1, "12".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 2, "3".getBytes))
    )
  }

  private def seekAndRead(inputStream: FSDataInputStream, pos: Long, buffer: Array[Byte]): String = {
    inputStream.seek(pos)
    inputStream.read(buffer)
    new String(buffer)
  }

  it should "seek" in {
    val (_, tablePath) = getNameAndFileLocation("testLog")
    writeSingleStringToLog(tablePath, "1_2_3_4_5_6_7_8_9_10_11_12_13_14_15_16_17_18_19_20")

    val buffer = new Array[Byte](3)
    val in = fs.open(new Path(tablePath))

    seekAndRead(in, 0, buffer) shouldEqual "1_2"
    seekAndRead(in, 10, buffer) shouldEqual "6_7"
    seekAndRead(in, 2, buffer) shouldEqual "2_3"
    seekAndRead(in, 23, buffer) shouldEqual "_12"
  }

  it should "overwrite existed file" in {
    val (fileName, tablePath) = getNameAndFileLocation("testLog")

    writeSingleStringToLog(tablePath, "123")
    writeSingleStringToLog(tablePath, "345")
    val id = getId(tableLocation, fileName)

    val res = getAllRows(tableLocation, schema).map(x => YtEventLogBlockTest(YtEventLogBlock(x)))
    res should contain theSameElementsAs Seq(
      YtEventLogBlockTest(YtEventLogBlock(id, 1, "34".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 2, "5".getBytes))
    )
  }

  private def getMetaByFileName(fileName: String): Seq[YtEventLogFileMeta] = {
    YtWrapper.selectRows(hadoopPathToYt(metaTableLocation), metaSchema,
      Some(s"""$FILENAME="$fileName"""")).map(YtEventLogFileDetails(_).meta)
  }

  private def getAllRows(path: String, schema: TableSchema): Seq[String] = {
    YtWrapper.selectRows(hadoopPathToYt(path), schema, None).map(YTreeTextSerializer.serialize(_))
  }

  private def getAllLogBlocks(path: String): Seq[YtEventLogBlockTest] = {
    getAllRows(path, schema).map(x => YtEventLogBlockTest(YtEventLogBlock(x)))
  }

  it should "multiple write and flush" in {
    val (fileName, tablePath) = getNameAndFileLocation("testLog")

    val out = fs.create(new Path(tablePath))
    try {
      out.write("123".getBytes())
      out.write("4567".getBytes())
      out.flush()
      out.flush()
      out.write("89".getBytes())
      out.flush()
    } finally {
      out.close()
    }
    val id = getId(tableLocation, fileName)

    getAllLogBlocks(tableLocation) should contain theSameElementsAs Seq(
      YtEventLogBlockTest(YtEventLogBlock(id, 1, "12".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 2, "34".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 3, "56".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 4, "78".getBytes)),
      YtEventLogBlockTest(YtEventLogBlock(id, 5, "9".getBytes))
    )
  }

  it should "don't flush after flush spam" in {
    val fsConf = {
      val c = new Configuration()
      c.set("yt.dynTable.rowSize", "6")
      c.set("fs.ytEventLog.singleReadLimit", "6")
      c.set("yt.proxy", "localhost:8000")
      c.set("yt.user", "root")
      c.set("yt.token", "")
      c
    }
    val fs = new YtEventLogFileSystem
    fs.initialize(new Path("/").toUri, fsConf)
    fs.setConf(fsConf)

    val (fileName, tablePath) = getNameAndFileLocation("testLog")

    val out = fs.create(new Path(tablePath))
    val id = getId(tableLocation, fileName)

    try {
      (0 until 5).foreach {
        i =>
          out.write(i.toString.getBytes())
          out.flush()
      }
      getAllLogBlocks(tableLocation) should contain theSameElementsAs Seq(
        YtEventLogBlockTest(YtEventLogBlock(id, 1, "0123".getBytes))
      )

      // forced flush when buffer is full
      out.write(5.toString.getBytes())
      out.flush()
      getAllLogBlocks(tableLocation) should contain theSameElementsAs Seq(
        YtEventLogBlockTest(YtEventLogBlock(id, 1, "012345".getBytes))
      )
    } finally {
      out.close()
    }
  }

  it should "force flush on close" in {
    val fsConf = {
      val c = new Configuration()
      c.set("yt.dynTable.rowSize", "6")
      c.set("fs.ytEventLog.singleReadLimit", "6")
      c.set("yt.proxy", "localhost:8000")
      c.set("yt.user", "root")
      c.set("yt.token", "")
      c
    }
    val fs = new YtEventLogFileSystem
    fs.initialize(new Path("/").toUri, fsConf)
    fs.setConf(fsConf)

    val (fileName, tablePath) = getNameAndFileLocation("testLog")

    val out = fs.create(new Path(tablePath))
    val id = getId(tableLocation, fileName)

    try {
      (0 until 5).foreach {
        i =>
          out.write(i.toString.getBytes())
          out.flush()
      }
      getAllLogBlocks(tableLocation) should contain theSameElementsAs Seq(
        YtEventLogBlockTest(YtEventLogBlock(id, 1, "0123".getBytes))
      )
    } finally {
      out.close()
    }
    // forced flush after close
    getAllLogBlocks(tableLocation) should contain theSameElementsAs Seq(
      YtEventLogBlockTest(YtEventLogBlock(id, 1, "01234".getBytes))
    )
  }

  it should "write meta data" in {
    val (fileName1, tablePath1) = getNameAndFileLocation("testLog1")
    val out1 = fs.create(new Path(tablePath1))

    val (fileName2, tablePath2) = getNameAndFileLocation("testLog2")
    val out2 = fs.create(new Path(tablePath2))

    val (fileName3, tablePath3) = getNameAndFileLocation("testLog3")
    fs.create(new Path(tablePath3)).close()

    try {
      out1.write("123".getBytes())
      out2.write("4567".getBytes())
      out1.flush()
      out1.flush()
      out1.write("89".getBytes())
      out2.flush()
    } finally {
      out1.close()
      out2.close()
    }

    val res = (getMetaByFileName(fileName1) ++ getMetaByFileName(fileName2) ++ getMetaByFileName(fileName3))
      .map(x => (x.rowSize, x.blocksCnt, x.length, x.modificationTs))
    res should contain theSameElementsAs Seq(
      (2, 3, 5, 1625486400000L),
      (2, 2, 4, 1625486400000L),
      (2, 0, 0, 1625486400000L),
    )
  }

  it should "fileStatus" in {
    val dirPath = tableLocation

    val (_, tablePath1) = getNameAndFileLocation("testLog1")
    writeSingleStringToLog(tablePath1, "123")

    val (_, tablePathUnknown) = getNameAndFileLocation("testLog")

    val validQueries = List(tablePath1, dirPath)
    val notValidQueries = List(tablePathUnknown)

    notValidQueries.foreach { q =>
      a[FileNotFoundException] shouldBe thrownBy {
        fs.getFileStatus(new Path(q))
      }
    }
    val res = validQueries.map(x => fs.getFileStatus(new Path(x))).map(unpackFileStatus)
    res should contain theSameElementsAs Seq(
      (new Path(dirPath), true, 0),
      (new Path(tablePath1), false, 3)
    )
  }

  private def unpackFileStatus(fileStatus: FileStatus): (Path, Boolean, Long) = {
    (fileStatus.getPath, fileStatus.isDirectory, fileStatus.getLen)
  }

  it should "listStatus" in {
    fs.setClock(Clock.systemUTC())

    val (_, tablePath1) = getNameAndFileLocation("testLog1")
    fs.create(new Path(tablePath1)).close()

    val (_, tablePath2) = getNameAndFileLocation("testLog2")
    writeSingleStringToLog(tablePath2, "1")

    val list = fs.listStatus(new Path(tableLocation))
    val res = list.map(unpackFileStatus)
    val mts = list.map(l => l.getModificationTime)

    res should contain theSameElementsAs Seq(
      (new Path(tablePath1), false, 0),
      (new Path(tablePath2), false, 1)
    )
    mts shouldEqual mts.sorted
  }

  private def renameAndCheckExisting(tablePathOld: Path, tablePathNew: Path, fileSize: Long) = {
    fs.rename(tablePathOld, tablePathNew)

    val res = fs.listStatus(new Path(tableLocation)).map(unpackFileStatus)

    res should contain theSameElementsAs Seq(
      (tablePathNew, false, fileSize)
    )
  }

  it should "exists" in {
    val (_, tablePath1) = getNameAndFileLocation("testLog1")
    val (_, tablePath2) = getNameAndFileLocation("testLog2")
    fs.create(new Path(tablePath1)).close()

    fs.exists(new Path(tablePath1)) shouldEqual true
    fs.exists(new Path(tablePath2)) shouldEqual false
    fs.exists(new Path(tableLocation)) shouldEqual true
  }

  it should "mkdirs" in {
    fs.mkdirs(new Path(tableLocation))

    val fileStatus = unpackFileStatus(fs.getFileStatus(new Path(tableLocation)))
    fileStatus shouldEqual(new Path(tableLocation), true, 0)

    val listDirectory = fs.listStatus(new Path(tableLocation))
    val resDirectory = listDirectory.map(unpackFileStatus)
    resDirectory.isEmpty shouldEqual true
  }

  it should "throw a correct exception after failed mkdirs" in {
    val (_, tablePath) = getNameAndFileLocation("testLog")

    YtWrapper.createDynTable(hadoopPathToYt(tableLocation), schema)
    an[IllegalArgumentException] shouldBe thrownBy {
      fs.listStatus(new Path(tableLocation))
    }
    a[FileNotFoundException] shouldBe thrownBy {
      fs.getFileStatus(new Path(tablePath))
    }

    YtWrapper.mountAndWait(hadoopPathToYt(tableLocation))
    an[IllegalArgumentException] shouldBe thrownBy {
      fs.listStatus(new Path(tableLocation))
    }
    a[FileNotFoundException] shouldBe thrownBy {
      fs.getFileStatus(new Path(tablePath))
    }
  }

  it should "rename" in {
    val (_, tablePathOld) = getNameAndFileLocation("testLog2")
    writeSingleStringToLog(tablePathOld, "123")
    val (_, tablePathNew) = getNameAndFileLocation("testLog3")

    renameAndCheckExisting(new Path(tablePathOld), new Path(tablePathNew), 3)
  }

  it should "rename empty file" in {
    val (_, tablePathOld) = getNameAndFileLocation("testLog1")
    fs.create(new Path(tablePathOld)).close()
    val (_, tablePathNew) = getNameAndFileLocation("testLog4")

    renameAndCheckExisting(new Path(tablePathOld), new Path(tablePathNew), 0)
  }

  it should "write and read" in {
    val (_, tablePath1) = getNameAndFileLocation("testLog1")
    val (_, tablePath2) = getNameAndFileLocation("testLog2")

    val out1 = fs.create(new Path(tablePath1))
    val out2 = fs.create(new Path(tablePath2))
    try {
      out1.write("123".getBytes())
      out2.write("4567".getBytes())
      out2.flush()
      out1.write("8".getBytes())
      out1.flush()
      out2.write("9".getBytes())
    } finally {
      out1.close()
      out2.close()
    }

    readAllData(tablePath1) shouldEqual "1238"
    readAllData(tablePath2) shouldEqual "45679"
  }

  def readAllData(path: String): String = {
    val in1 = fs.open(new Path(path))
    try {
      val source = Source.fromInputStream(in1)
      try {
        source.mkString
      } finally source.close()
    } finally in1.close()
  }

  it should "delete" in {
    val (_, tablePath1) = getNameAndFileLocation("testLog1")
    val (_, tablePath2) = getNameAndFileLocation("testLog2")
    val (_, tablePath3) = getNameAndFileLocation("testLog3")

    fs.create(new Path(tablePath1)).close()
    writeSingleStringToLog(tablePath2, "4567")

    fs.delete(new Path(tablePath1), recursive = false) shouldEqual true
    fs.delete(new Path(tablePath2), recursive = true) shouldEqual true
    fs.delete(new Path(tablePath3), recursive = false) shouldEqual false

    fs.listStatus(new Path(tableLocation)).isEmpty shouldEqual true
  }
}
