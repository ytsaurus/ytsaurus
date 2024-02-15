package org.apache.spark.sql.v2

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.v2.Utils.getStatistics
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

class YtTableStatisticsTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with MockitoSugar with DynTableTestUtils {
  import spark.implicits._

  protected val conf = Map(
    WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
    CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.cbo.enabled" -> "true",
    "spark.sql.cbo.joinReorder.enabled" -> "true",
  )

  it should "calc table statistics" in {
    val data = (0 until 123).map(x => (x / 100, x / 10, x, -x))
    data.toDF("a", "b", "c", "d").write.yt(tmpPath)
    val size = YtWrapper.fileSize(tmpPath, None)

    val statistics = getStatistics(spark.read.yt(tmpPath))
    statistics.numRows().getAsLong shouldBe 123
    statistics.sizeInBytes().getAsLong shouldBe size
  }

  it should "calc table statistics for directory" in {
    YtWrapper.createDir(tmpPath)
    (0 until 150).toDF("a").write.yt(s"$tmpPath/t1")
    (0 until 71).map(x => -x).toDF("a").write.yt(s"$tmpPath/t2")
    val size = YtWrapper.fileSize(s"$tmpPath/t1", None) + YtWrapper.fileSize(s"$tmpPath/t2", None)

    val statistics = getStatistics(spark.read.yt(tmpPath))
    statistics.numRows().getAsLong shouldBe 221
    statistics.sizeInBytes().getAsLong shouldBe size
  }

  private def findAllScans(query: DataFrame): Seq[YtScan] = {
    query.queryExecution.executedPlan.collect {
      case s: BatchScanExec => s.scan.asInstanceOf[YtScan]
    }
  }

  it should "reorder joins" in {
    withConfs(conf) {
      YtWrapper.createDir(tmpPath)
      (0 until 100).map(x => (x / 10, -x)).toDF("a", "b").write.yt(s"$tmpPath/t1")
      (0 until 20).map(x => (x / 2, x)).toDF("a", "c").write.yt(s"$tmpPath/t2")
      (0 until 5).map(x => (x, "string")).toDF("a", "d").write.yt(s"$tmpPath/t3")

      val query = spark.read.yt(s"$tmpPath/t1")
        .join(spark.read.yt(s"$tmpPath/t2"), "a")
        .join(spark.read.yt(s"$tmpPath/t3"), "a")
      val res = query.collect()
      val expected = (0 until 100).map(x => Row(x / 20, -x / 2, 2 * (x / 20) + x % 2, "string"))
      res should contain theSameElementsAs expected

      val files = findAllScans(query).map(_.getPartitions.head.files.head.filePath).map(new Path(_).getName)
      files should contain theSameElementsAs Seq("t1", "t2", "t3")
      files shouldNot contain theSameElementsInOrderAs Seq("t1", "t2", "t3")
    }
  }
}
