package ru.yandex.spark.yt.logger

import org.apache.log4j.Level
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.Read.KeyColumnsFilterPushdown
import ru.yandex.spark.yt.test._
import ru.yandex.spark.yt.wrapper.YtWrapper

import java.util.UUID
import scala.concurrent.duration._
import scala.language.postfixOps

class YtDynTableLoggerTest extends FlatSpec with Matchers with LocalSpark with TmpDir with DynTableTestUtils with TestUtils {

  import SparkComponent._
  import ru.yandex.spark.yt.fs.conf._
  import spark.implicits._

  val tmpPathGlobal = s"$testDir/test-${UUID.randomUUID()}"

  "YtDynTableLogger" should "write logs to table" in {
    spark.conf.set("spark.yt.log.level", "INFO")
    spark.conf.set("spark.yt.log.test2.level", "WARN")
    val log1 = YtDynTableLogger.fromSpark("test1", spark)
    val log2 = YtDynTableLogger.fromSpark("test2", spark)

    log1.trace("trace test msg 1")
    log2.trace("trace test msg 2")
    log1.debug("debug test msg 1")
    log2.debug("debug test msg 2")
    log1.info("info test msg 1")
    log2.info("info test msg 2")
    log1.warn("warn test msg 1")
    log2.warn("warn test msg 2")
    log1.error("error test msg 1")
    log2.error("error test msg 2")

    parseLogs(YtWrapper.selectRows(tmpPathGlobal)) should contain theSameElementsAs Seq(
      LogRow("test1", "info test msg 1", Level.INFO, Driver),
      LogRow("test1", "warn test msg 1", Level.WARN, Driver),
      LogRow("test1", "error test msg 1", Level.ERROR, Driver),

      LogRow("test2", "warn test msg 2", Level.WARN, Driver),
      LogRow("test2", "error test msg 2", Level.ERROR, Driver),
    )
  }

  it should "write executor logs" in {
    val df = (1 to 100).toDS().repartition(10)
    val logConf = YtDynTableLoggerConfig.fromSpark(spark)

    df.map { i =>
      val log = YtDynTableLogger.fromConfig("test", logConf)
      log.info(s"Log $i")
      i
    }.collect()

    parseLogs(YtWrapper.selectRows(tmpPathGlobal)) should contain theSameElementsAs (1 to 100).map(i =>
      LogRow("test", s"Log $i", Level.INFO, Executor)
    )
  }

  it should "get info from task context" in {
    val df = (1 to 100).toDS().coalesce(1)
    val logConf = YtDynTableLoggerConfig.fromSpark(spark)

    df.mapPartitions { iter =>
      val taskContext = TaskInfo(1, 1)
      val log = YtDynTableLogger.fromConfig("test", logConf.map(_.copy(taskContext = Some(taskContext))))
      iter.map { i =>
        log.info(s"Log $i")
        i
      }
    }.collect()

    parseLogs(YtWrapper.selectRows(tmpPathGlobal)) should contain theSameElementsAs (1 to 100).map(i =>
      LogRow("test", s"Log $i", Level.INFO, Executor, Some(1))
    )
  }

  it should "write logs while reading dataset" in {
    import org.apache.spark.sql.internal.SQLConf.FILES_OPEN_COST_IN_BYTES
    import ru.yandex.spark.yt.{YtReader, YtWriter}
    (1 to 100).toDF().sort("value").coalesce(2).write.sortedBy("value").yt(tmpPath)

    withConf(KeyColumnsFilterPushdown.Enabled, true) {
      withConf(FILES_OPEN_COST_IN_BYTES, "1") {
        spark.read.option("readParallelism", "2").yt(tmpPath).filter('value > 50).collect()
      }
    }

    val logs = parseLogs(YtWrapper.selectRows(tmpPathGlobal)).map(_.copy(msg = "", info = Map.empty))

    logs.exists(_.sparkComponent == SparkComponent.Driver) shouldEqual true
    logs.exists(_.sparkComponent == SparkComponent.Executor) shouldEqual true
  }

  it should "force trace level for extra executor logs" in {
    import org.apache.spark.sql.internal.SQLConf.FILES_OPEN_COST_IN_BYTES
    import ru.yandex.spark.yt.{YtReader, YtWriter}
    (1 to 100).toDF().sort("value").coalesce(2).write.sortedBy("value").yt(tmpPath)

    withConf("spark.yt.log.pushdown.maxPartitionId", "0") {
      withConf(KeyColumnsFilterPushdown.Enabled, true) {
        withConf(FILES_OPEN_COST_IN_BYTES, "1") {
          spark.read.option("readParallelism", "2").yt(tmpPath).filter('value > 50).collect()
        }
      }
    }

    val logs = parseLogs(YtWrapper.selectRows(tmpPathGlobal)).map(_.copy(msg = "", info = Map.empty))

    logs.exists(_.sparkComponent == SparkComponent.Driver) shouldEqual true
    logs.exists(_.sparkComponent == SparkComponent.Executor) shouldEqual true
    logs.filter(_.sparkComponent == SparkComponent.Executor).foreach(_.partitionId shouldEqual Some(0))
  }

  it should "log map as yson" in {
    val log = YtDynTableLogger.fromSpark("test", spark)
    val info = Map("a" -> "aaa", "b" -> "bbb")

    log.info("test message", info)

    parseLogs(YtWrapper.selectRows(tmpPathGlobal)) should contain theSameElementsAs Seq(
      LogRow("test", "test message", Level.INFO, Driver, None, info)
    )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.base.discovery.path", "test_discovery_path")
    spark.conf.set("spark.yt.version", "1.2.3")
    spark.setYtConf(SparkYtLogConfiguration.Enabled, true)

    YtWrapper.createTable(tmpPathGlobal, TestTableSettings(YtDynTableLogger.logTableSchema, isDynamic = true))
    YtWrapper.mountTableSync(tmpPathGlobal, 5 seconds)
    spark.setYtConf(SparkYtLogConfiguration.Table, tmpPathGlobal)
  }

  override def afterAll(): Unit = {
    spark.setYtConf(SparkYtLogConfiguration.Enabled, false)
    YtWrapper.unmountTableSync(tmpPathGlobal, 5 seconds)
    YtWrapper.remove(tmpPathGlobal)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    import scala.collection.JavaConverters._
    val allRows = YtWrapper.selectRows(tmpPathGlobal)
    allRows.foreach { row =>
      val keys = YtDynTableLogger.logTableSchema.toKeys
        .getColumnNames.asScala
        .map(name => name -> row.getString(name).asInstanceOf[Any])
        .toMap
      YtWrapper.deleteRow(tmpPathGlobal, YtDynTableLogger.logTableSchema, keys.asJava)
    }
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  private def parseLogs(rawLogs: Seq[YTreeMapNode]): Seq[LogRow] = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._

    import scala.collection.JavaConverters._
    rawLogs.map(row =>
      LogRow(
        row.getString("logger_name"),
        row.getString("msg"),
        Level.toLevel(row.getString("level")),
        SparkComponent.fromName(row.getString("spark_component")),
        toOption(row.getIntO("partition_id")).map(_.toInt),
        row.getMap("info")
          .asMap().asScala.toMap
          .mapValues(_.stringValue())
      )
    )
  }
}
