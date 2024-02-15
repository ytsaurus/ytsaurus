package tech.ytsaurus.spyt.format

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.spyt.test.{DynTableTestUtils, TestRow}

import scala.concurrent.duration._
import scala.language.postfixOps

class YtReadLockTest extends FlatSpec with Matchers with LocalSpark with TestUtils with TmpDir with DynTableTestUtils {
  behavior of "YtDataSource"

  import YtWrapper._
  import spark.implicits._

  it should "not fail if table was modified while reading" in {
    (1 to 10).toDF().write.yt(tmpPath)

    val df = spark.read.yt(tmpPath)
    (11 to 30).toDF().write.mode(SaveMode.Append).yt(tmpPath)

    noException shouldBe thrownBy {
      df.as[Long].collect()
    }
  }

  it should "fail if table was deleted while reading" in {
    (1 to 10).toDF().write.yt(tmpPath)

    val df = spark.read.yt(tmpPath)
    remove(tmpPath)

    a[RuntimeException] shouldBe thrownBy {
      df.as[Long].collect()
    }
  }

  it should "lock dataset while reading arrow" in {
    createDir(tmpPath)
    (1 to 10).toDF().write.optimizeFor(OptimizeMode.Scan).yt(s"$tmpPath/1")
    (20 to 50).toDF().write.optimizeFor(OptimizeMode.Scan).yt(s"$tmpPath/2")

    readWithinTransaction(s"$tmpPath/1") { (df, _) =>
      move(s"$tmpPath/2", s"$tmpPath/1", force = true)

      df.as[Long].collect() should contain theSameElementsAs (1 to 10)
    }
  }

  it should "lock dataset while reading wire protocol" in {
    createDir(tmpPath)
    (1 to 10).toDF().write.optimizeFor(OptimizeMode.Lookup).yt(s"$tmpPath/1")
    (20 to 50).toDF().write.optimizeFor(OptimizeMode.Lookup).yt(s"$tmpPath/2")

    readWithinTransaction(s"$tmpPath/1") { (df, _) =>
      move(s"$tmpPath/2", s"$tmpPath/1", force = true)

      df.as[Long].collect() should contain theSameElementsAs (1 to 10)
    }
  }

  it should "lock dataset while counting" in {
    createDir(tmpPath)
    (1 to 10).toDF().write.yt(s"$tmpPath/1")
    (20 to 50).toDF().write.yt(s"$tmpPath/2")

    readWithinTransaction(s"$tmpPath/1") { (df, _) =>
      move(s"$tmpPath/2", s"$tmpPath/1", force = true)

      df.as[Long].count() shouldEqual 10
    }
  }

  it should "lock directory while reading" in {
    createDir(tmpPath)
    (1 to 10).toDF().write.yt(s"$tmpPath/1")
    (11 to 50).toDF().write.yt(s"$tmpPath/2")

    readWithinTransaction(tmpPath) { (df, _) =>
      move(s"$tmpPath/2", s"$tmpPath/1", force = true)

      df.as[Long].collect() should contain theSameElementsAs (1 to 50)
    }
  }

  it should "release lock after reading" in {
    (1 to 10).toDF().write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Long].collect() should contain theSameElementsAs (1 to 10)
    lockCount(tmpPath) shouldEqual 0
  }

  it should "keep lock until transaction is committed" in {
    createDir(tmpPath)
    (1 to 10).toDF().write.yt(s"$tmpPath/1")
    (20 to 50).toDF().write.yt(s"$tmpPath/2")
    lockCount(s"$tmpPath/1") shouldEqual 0
    readWithinTransaction(s"$tmpPath/1") { (df, readTransaction) =>
      lockCount(s"$tmpPath/1") shouldEqual 1

      val path = objectPath(s"$tmpPath/1", readTransaction)
      lockCount(path) shouldEqual 1

      move(s"$tmpPath/2", s"$tmpPath/1", force = true)
      df.as[Long].collect() should contain theSameElementsAs (1 to 10)

      lockCount(path) shouldEqual 1
    }
  }

  it should "not lock table if listing was done on executors" in {
    createDir(tmpPath)
    val tableCount = 3
    (1 to tableCount).foreach(i =>
      (10 * i to 10 * (i + 1)).toDF().write.yt(s"$tmpPath/$i")
    )

    val df = withConf(PARALLEL_PARTITION_DISCOVERY_THRESHOLD, "2") {
      spark.read.yt((1 to tableCount).map(i => s"$tmpPath/$i"): _*)
    }
    (31 to 50).toDF().write.mode(SaveMode.Append).yt(s"$tmpPath/1")

    noException shouldBe thrownBy {
      df.as[Long].collect()
    }
  }

  it should "lock table within transaction if listing was done on executors" in {
    createDir(tmpPath)
    val tableCount = 3
    (1 to tableCount).foreach(i =>
      (10 * i - 9 to 10 * i).toDF().write.yt(s"$tmpPath/$i")
    )
    withConf(PARALLEL_PARTITION_DISCOVERY_THRESHOLD, "2") {
      readWithinTransaction((1 to tableCount).map(i => s"$tmpPath/$i"): _*) { (df, _) =>
        move(s"$tmpPath/2", s"$tmpPath/1", force = true)

        df.as[Long].collect() should contain theSameElementsAs (1 to 30)
      }
    }
  }

  it should "lock csv files" in {
    writeFileFromString(
      """a,b,c
        |1,2,3
        |4,5,6""".stripMargin,
      tmpPath
    )

    val tr = createTransaction(None, 5 minutes)
    try {
      val res = spark.read
        .option("header", "true")
        .csv(s"${tmpPath.drop(1)}/@transaction_${tr.getId.toString}")

      remove(tmpPath)
      writeFileFromString(
        """d,e
          |10,11
          |12,13""".stripMargin,
        tmpPath
      )

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row("1", "2", "3"),
        Row("4", "5", "6")
      )
    } finally {
      tr.commit()
    }
  }

  it should "read dynamic table snapshot with custom timestamp" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)

    val ts = yt.generateTimestamps().join().getValue
    insertNewRow(tmpPath)
    val df = spark.read.option("timestamp", ts).yt(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read dynamic table snapshot" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)

    val df = spark.read.yt(tmpPath)
    insertNewRow(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read latest version of dynamic table if timestamp is disabled by user" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)

    val df = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
    insertNewRow(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs TestRow(105, 105, "new_new_row") +: testData
  }

  it should "set snapshot lock on dynamic table within transaction" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))

    readWithinTransaction(tmpPath) { (df, readTransaction) =>
      val path = objectPath(tmpPath, readTransaction)
      lockCount(path) shouldEqual 1

      remove(tmpPath)

      df.selectAs[TestRow].collect() should contain theSameElementsAs testData
      lockCount(path) shouldEqual 1
    }
  }

  it should "read dynamic table snapshot with custom timestamp within transaction" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)
    val ts = yt.generateTimestamps().join().getValue

    readCustomTimestampWithinTransaction(ts, tmpPath) { (df, readTransaction) =>
      lockCount(objectPath(tmpPath, readTransaction)) shouldEqual 1

      insertNewRow(tmpPath)
      remove(tmpPath)

      df.selectAs[TestRow].collect() should contain theSameElementsAs testData
    }
  }

  it should "read directory of dynamic tables" in {
    YtWrapper.createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    prepareTestTable(s"$tmpPath/2", Seq(testRow), Nil)

    val df = spark.read.yt(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
  }

  it should "set snapshot lock on directory of dynamic tables" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    prepareTestTable(s"$tmpPath/2", Seq(testRow), Nil)

    readWithinTransaction(tmpPath) { (df, readTransaction) =>
      lockCount(objectPath(s"$tmpPath/1", readTransaction)) shouldEqual 1
      lockCount(objectPath(s"$tmpPath/2", readTransaction)) shouldEqual 1

      remove(s"$tmpPath/2")

      df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
    }
  }

  it should "read directory of dynamic tables with custom timestamp within transaction" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)
    prepareTestTable(s"$tmpPath/2", Seq(testRow), Nil, enableDynamicStoreRead = true)
    val ts = yt.generateTimestamps().join().getValue

    insertNewRow(s"$tmpPath/1")

    readCustomTimestampWithinTransactionRecursive(ts, tmpPath) { (df, readTransaction) =>
      lockCount(objectPath(s"$tmpPath/1", readTransaction)) shouldEqual 1
      lockCount(objectPath(s"$tmpPath/2", readTransaction)) shouldEqual 1

      remove(s"$tmpPath/2")

      df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
    }
  }

  it should "read directory of dynamic tables with custom timestamp" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true)
    prepareTestTable(s"$tmpPath/2", Seq(testRow), Nil, enableDynamicStoreRead = true)

    val ts = yt.generateTimestamps().join().getValue
    insertNewRow(s"$tmpPath/1")

    val df = spark.read.timestamp(ts).yt(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
  }

  it should "read directory of both static and dynamic tables" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true) // dynamic
    Seq(testRow).toDF().write.yt(s"$tmpPath/2") //static

    val df = spark.read.yt(tmpPath)

    df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
  }

  it should "read directory of both static and dynamic tables within transaction" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true) // dynamic
    Seq(testRow).toDF().write.yt(s"$tmpPath/2") //static

    readWithinTransaction(tmpPath) { (df, _) =>
      remove(s"$tmpPath/2")

      df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
    }
  }

  it should "read directory of both static and dynamic tables within transaction with custom timestamp" in {
    createDir(tmpPath)
    prepareTestTable(s"$tmpPath/1", testData, Seq(Seq(), Seq(3), Seq(6, 12)), enableDynamicStoreRead = true) // dynamic
    Seq(testRow).toDF().write.yt(s"$tmpPath/2") //static

    val ts = yt.generateTimestamps().join().getValue
    insertNewRow(s"$tmpPath/1")

    readCustomTimestampWithinTransactionRecursive(ts, tmpPath) { (df, _) =>
      remove(s"$tmpPath/2")

      df.selectAs[TestRow].collect() should contain theSameElementsAs testRow +: testData
    }
  }

  private def readWithinTransaction(path: String*)
                                   (f: (DataFrame, String) => Unit): Unit = {
    readWithinTransactionInner(identity, path:_*)(f)
  }

  private def readCustomTimestampWithinTransaction(timestamp: Long, path: String*)
                                                  (f: (DataFrame, String) => Unit): Unit = {
    readWithinTransactionInner(_.timestamp(timestamp), path:_*)(f)
  }

  private def readCustomTimestampWithinTransactionRecursive(timestamp: Long, path: String*)
                                                  (f: (DataFrame, String) => Unit): Unit = {
    readWithinTransactionInner(_.timestamp(timestamp).option("recursiveFileLookup", "true"), path:_*)(f)
  }

  private def readWithinTransactionInner(applyOptions: DataFrameReader => DataFrameReader, path: String*)
                                   (f: (DataFrame, String) => Unit): Unit = {
    val readTransaction = createTransaction(None, 5 minutes)
    try {
      val reader = spark.read.transaction(readTransaction.getId.toString)
      val df = applyOptions(reader).yt(path: _*)
      f(df, readTransaction.getId.toString)
    } finally {
      readTransaction.commit()
    }
  }

  private def insertNewRow(path: String): Unit = {
    YtWrapper.insertRows(path, testSchema, Seq(Seq(105, 105, "new_new_row")))
  }
}
