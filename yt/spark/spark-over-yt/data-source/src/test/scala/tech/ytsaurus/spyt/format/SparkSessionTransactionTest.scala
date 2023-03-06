package tech.ytsaurus.spyt.format

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_LISTENERS
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.GlobalTransaction
import tech.ytsaurus.spyt.fs.conf.{SparkYtSparkConf, SparkYtSparkSession}
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.test.LocalYtClient

import scala.language.postfixOps

class SparkSessionTransactionTest extends FlatSpec with Matchers with LocalYtClient with TmpDir with TestUtils {
  behavior of "SparkSession"

  it should "create transaction and commit it when session is closed" in {
    writeTableFromYson(Seq(
      """{value = 1}""",
      """{value = 2}"""
    ), tmpPath, longColumnSchema)
    val spark = prepareSparkSession(true)
    import spark.implicits._

    val transaction = try {
      val tr = spark.ytConf(GlobalTransaction.Id)
      YtWrapper.transactionExists(tr) shouldBe true
      val df = spark.read.yt(tmpPath)
      YtWrapper.lockCount(tmpPath) shouldEqual 1

      YtWrapper.remove(tmpPath)

      df.as[Long].collect() should contain theSameElementsAs Seq(1, 2)

      tr
    } finally {
      spark.stop()
    }
    YtWrapper.transactionExists(transaction) shouldBe false
  }

  it should "not create transaction if global transaction option is disabled" in {
    writeTableFromYson(Seq(
      """{value = 1}""",
      """{value = 2}"""
    ), tmpPath, longColumnSchema)
    val spark = prepareSparkSession(false)
    try {
      spark.getYtConf(GlobalTransaction.Id).isEmpty shouldBe true
    } finally {
      spark.stop()
    }
  }

  private def prepareSparkSession(globalTransactionEnabled: Boolean): SparkSession = {
    LocalSpark.stop()
    val sparkConf = LocalSpark.defaultSparkConf
      .clone()
      .setYtConf(GlobalTransaction.Enabled, globalTransactionEnabled)
      .set(SPARK_SESSION_LISTENERS.key, classOf[GlobalTransactionSparkListener].getCanonicalName)
    SparkSession.builder().master(s"local[1]").config(sparkConf).getOrCreate()
  }
}
