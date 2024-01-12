package tech.ytsaurus.spyt.fs.streaming.state

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.test.{LocalSpark, LocalYtClient, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.DurationInt


class YTsaurusStreamingTest extends FlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils with TmpDir {
  import spark.implicits._
  import tech.ytsaurus.spyt._

  it should "work with native key-value storage and FileContext YTsaurus API" in {
    val batchCount = 3L
    val batchSeconds = 5

    YtWrapper.createDir(tmpPath)

    val numbers = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select($"timestamp", floor(rand() * 10).as("num"))

    val stopSignal = Promise[Unit]()

    val groupedNumbers = numbers
      .withWatermark("timestamp", "5 seconds")
      .groupBy(window($"timestamp", "5 seconds", "3 seconds"), $"num")
      .count()

    val job = groupedNumbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .trigger(ProcessingTime(batchSeconds * 1000))
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        if (batchNum >= batchCount) {
          if (!stopSignal.isCompleted) stopSignal.success()
          ()
        } else {
          frame.write.mode(SaveMode.Append).yt(s"$tmpPath/result")
        }
      }

    val query = job.start()
    Await.result(stopSignal.future, 420 seconds)
    query.stop()

    val resultDF = spark.read.yt(s"$tmpPath/result")
    val receivedNums = resultDF.select(sum("count").cast("long")).first().getLong(0)
    receivedNums should be >= ((batchCount - 1) * batchSeconds)
  }
}
