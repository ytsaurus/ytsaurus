package tech.ytsaurus.spyt.format

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}

import scala.language.postfixOps

class AdaptiveTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TableDrivenPropertyChecks {

  import spark.implicits._

  it should "split skew partitions" in {
    withConf(AUTO_BROADCASTJOIN_THRESHOLD, "-1") {
      withConf(ADVISORY_PARTITION_SIZE_IN_BYTES, "1b") {
        withConf(SKEW_JOIN_SKEWED_PARTITION_THRESHOLD, "1b") {
          withConf(ADAPTIVE_EXECUTION_ENABLED.key, "true") {
            val df1 = (Seq.fill(96)(1) ++ Seq(2, 3, 4, 5)).zip(11 to 110).toDF("key", "value1")
            val df2 = (1 to 10).zip(111 to 120).toDF("key", "value2")
            val join = df1.join(df2, Seq("key"))
            val plan = adaptivePlan(join)
            val planShufflePartitionsSpec = nodes(plan).collectFirst {
              case AQEShuffleReadExec(_, partitionSpecs) => partitionSpecs
            }.get

            val split = planShufflePartitionsSpec.collect {
              case p: PartialReducerPartitionSpec => p
            }
            split.length shouldEqual 4
          }
        }
      }
    }
  }

  it should "join df from yt" in {
    val df1 = (1 to 10).zip(11 to 20).toDF("key", "value1")
    val df2 = (1 to 10).zip(111 to 120).toDF("key", "value2")
    df1.coalesce(1).write.yt(s"$tmpPath/1")
    df2.coalesce(1).write.yt(s"$tmpPath/2")

    withConf(ADAPTIVE_EXECUTION_ENABLED.key, "true") {
      withConf(ADVISORY_PARTITION_SIZE_IN_BYTES, "1024b") {
        val expected = (1 to 10).zip(11 to 20).zip(111 to 120).map { case ((key, v1), v2) => Row(key, v1, v2) }
        val dfYt1 = spark.read.yt(s"$tmpPath/1")
        val dfYt2 = spark.read.yt(s"$tmpPath/2")

        val join = dfYt1.join(dfYt2, Seq("key"))
        join.write.yt(s"$tmpPath/3")

        join.select("key", "value1", "value2").collect() should contain theSameElementsAs expected
      }
    }
  }

  it should "group df from yt" in {
    val df1 = (1 to 10).zip(11 to 20).toDF("key", "value1")
    df1.repartition(10).write.yt(s"$tmpPath/1")
    val dfYt1 = spark.read.yt(s"$tmpPath/1")

    val res = dfYt1.groupBy("key").count()
    res.write.yt(s"$tmpPath/3")
  }


  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
}
