package tech.ytsaurus.spyt.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.util.UUID
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

class YTsaurusStreamingTest extends FlatSpec with Matchers with LocalSpark with LocalYtClient with TestUtils
  with TmpDir with DynTableTestUtils with QueueTestUtils {
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

  it should "write YT queue" in {
    val recordCountLimit = 50L

    YtWrapper.createDir(tmpPath)
    val path = s"$tmpPath/result-${UUID.randomUUID()}"

    val numbers = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select(floor(rand() * 10).as("num"))

    prepareOrderedTestTable(path, SchemaConverter.tableSchema(numbers.schema), enableDynamicStoreRead = true)

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .trigger(ProcessingTime(2000))
      .format("yt")
      .option("path", path)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        Thread.sleep(1000)
        currentCount = spark.read.option("enable_inconsistent_read", "true").yt(path).count()
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 150 seconds)
    query.stop()
  }

  it should "read YT queue" in {
    val recordCountLimit = 50L
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)
    prepareConsumer(consumerPath, queuePath)
    waitQueueRegistration(queuePath)

    val numbers = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .load(queuePath)

    val stopSignal = Promise[Unit]()
    var recordCount = 0L

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .trigger(ProcessingTime(2000))
      .foreachBatch { (frame: DataFrame, batchNum: Long) =>
        recordCount += frame.count()
        if (recordCount >= recordCountLimit && !stopSignal.isCompleted) stopSignal.success()
        ()
      }

    val recordFuture = Future[Unit] {
      while (!stopSignal.isCompleted) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(4000)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 120 seconds)
    query.stop()
  }

  it should "run pipeline on YT queues" in {
    val recordCountLimit = 50L
    val consumerPath = s"$tmpPath/consumer-${UUID.randomUUID()}"
    val queuePath = s"$tmpPath/inputQueue-${UUID.randomUUID()}"
    val resultPath = s"$tmpPath/result-${UUID.randomUUID()}"

    YtWrapper.createDir(tmpPath)
    prepareOrderedTestTable(queuePath, enableDynamicStoreRead = true)
    prepareConsumer(consumerPath, queuePath)
    waitQueueRegistration(queuePath)

    val numbers = spark
      .readStream
      .format("yt")
      .option("consumer_path", consumerPath)
      .load(queuePath)

    prepareOrderedTestTable(resultPath, SchemaConverter.tableSchema(numbers.schema), enableDynamicStoreRead = true)

    val job = numbers
      .writeStream
      .option("checkpointLocation", f"yt:/$tmpPath/stateStore")
      .trigger(ProcessingTime(2000))
      .format("yt")
      .option("path", resultPath)

    val recordFuture = Future[Unit] {
      var currentCount = 0L
      while (currentCount < recordCountLimit) {
        appendChunksToTestTable(queuePath, Seq(getTestData()), sorted = false, remount = false)
        Thread.sleep(2000)
        currentCount = spark.read.option("enable_inconsistent_read", "true").yt(resultPath).count()
        print(currentCount)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

    val query = job.start()
    Await.result(recordFuture, 120 seconds)
    query.stop()
  }
}
