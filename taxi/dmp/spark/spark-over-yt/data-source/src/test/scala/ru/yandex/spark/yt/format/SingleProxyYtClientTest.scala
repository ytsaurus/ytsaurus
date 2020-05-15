package ru.yandex.spark.yt.format

import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.{Partitioner, SparkConf}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.serializers.InternalRowDeserializer
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.{DefaultRpcCredentials, SingleProxyYtClient}
import ru.yandex.yt.ytclient.rpc.RpcCredentials

import scala.concurrent.duration._
import scala.language.postfixOps

class SingleProxyYtClientTest extends FlatSpec with Matchers with LocalSpark with TestUtils with TmpDir {
  import ru.yandex.spark.yt.wrapper.YtWrapper._

  val user = DefaultRpcCredentials.user
  val token = DefaultRpcCredentials.token

  def time[T](f: => T): T = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = (System.currentTimeMillis() - start).millis
    println(s"${duration.toSeconds} seconds")
    res
  }

  val eventLogSchema = StructType(Seq(
    StructField("moscow_event_dttm", StringType),
    StructField("adjust_id", StringType),
    StructField("advert_id", StringType),
    StructField("city", StringType),
    StructField("conversion_duration_sec", LongType),
    StructField("country_code", StringType),
    StructField("etl_updated", StringType),
    StructField("event_type", StringType),
    StructField("ip_address", StringType),
    StructField("is_organic_flg", BooleanType),
    StructField("match_type", StringType),
    StructField("moscow_click_time_dttm", StringType),
    StructField("moscow_created_at_dttm", StringType),
    StructField("moscow_installed_at_dttm", StringType),
    StructField("moscow_reattribitted_at_dttm", StringType),
    StructField("os_name", StringType),
    StructField("os_version", StringType),
    StructField("platform", StringType),
    StructField("region", StringType),
    StructField("service", StringType),
    StructField("service_code", StringType),
    StructField("tracker_id", StringType),
    StructField("tracker_name", StringType),
    StructField("user_id", StringType),
    StructField("utc_event_dttm", StringType),
    StructField("utm_campaign", StringType),
    StructField("utm_content", StringType),
    StructField("utm_source", StringType),
    StructField("utm_term", StringType)
  ))

  "SingleProxyYtClient" should "connect to hume proxies" ignore {

    val addresses = Seq(
      "man2-4288-436.hume.yt.gencfg-c.yandex.net:9013",
      "man2-4281-c3a.hume.yt.gencfg-c.yandex.net:9013"
    )
    val thread1 = new Thread(new Runnable {
      override def run(): Unit = {
        val deserialiser = InternalRowDeserializer.getOrCreate(eventLogSchema)
        val client = SingleProxyYtClient(addresses(1), DefaultRpcCredentials.credentials)
        val res = readTable(s"//home/sashbel/data/eventlog[#0:#${1}]", deserialiser)(client).toList
        println(res.length)
      }
    })

    time {
      thread1.start()
      thread1.join()
    }

    import spark.implicits._

    val part = new Partitioner {
      override def numPartitions: Int = 32

      override def getPartition(key: Any): Int = key.asInstanceOf[Int] % 32
    }

    (1 to 32).map(_ -> 0).toDS.rdd.partitionBy(part).collect()

    time {
      (29 to 42).map(_ -> 0).toDS.rdd.partitionBy(part).map{case (i, _) =>
        val deserialiser = InternalRowDeserializer.getOrCreate(eventLogSchema)
        val path = s"//home/sashbel/data/eventlog[#${i * 2 * 1024 * 1024}:#${(i + 1) * 2 * 1024 * 1024}]"
        val client = SingleProxyYtClient(addresses(i % 2), new RpcCredentials(user, token))
        val file = new File("test")
        if (file.exists()) file.delete()
        val out = new FileWriter(new File("test"))
        try {
          readTable(path, deserialiser)(client).foreach { row =>
            out.write(row.toSeq(eventLogSchema).mkString(","))
          }
        } finally {
          out.close()
        }
      }.collect()
    }
  }

  it should "work with dev host" in {
    val deserialiser = InternalRowDeserializer.getOrCreate(eventLogSchema)
    val client = SingleProxyYtClient("sashbel-dev.man.yp-c.yandex.net:27002", DefaultRpcCredentials.credentials)
    val res = readTable(s"//home/sashbel/data/eventlog[#0:#${1}]", deserialiser)(client).toList

    println(res)
  }

  it should "work with byop host" in {
    val deserialiser = InternalRowDeserializer.getOrCreate(eventLogSchema)
    val client = SingleProxyYtClient("man1-7550-acf.hume.yt.gencfg-c.yandex.net:27002", DefaultRpcCredentials.credentials)

    println(YtWrapper.exists("//home/sashbel")(client))
    val res = readTable(s"//home/sashbel/data/eventlog[#0:#${1}]", deserialiser)(client).toList

    println(res)
  }

  it should "work with executors connected to hume proxy" ignore {
    Logger.getRootLogger.setLevel(Level.INFO)
    spark.read.yt("//sys/spark/examples/example_1").show()
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.hadoop.yt.byop.enabled", "true")
    .set("spark.hadoop.yt.proxy", "hume")
    .set("spark.hadoop.yt.user", DefaultRpcCredentials.user)
    .set("spark.hadoop.yt.token", DefaultRpcCredentials.token)
}
