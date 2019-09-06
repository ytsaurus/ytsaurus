package ru.yandex.spark.yt

import java.time.Duration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}
import ru.yandex.yt.ytclient.proxy.TableReader
import ru.yandex.yt.ytclient.proxy.request.ReadTable
import ru.yandex.yt.ytclient.rpc.RpcCredentials

class YtRDD(@transient val sc: SparkContext,
            path: String,
            proxy: String,
            user: String,
            token: String,
            partitions: Int,
            schema: StructType,
            filters: Array[Filter]) extends RDD[Row](sc, Nil) {
  private val timeout = sc.getConf.get("spark.yt.timeout.minutes").toLong

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    split match {
      case partition: YtPartition =>
       // if (schema.isEmpty) {
       //   Iterator.fill((partition.endRowExclusive - partition.startRowInclusive).toInt)(Row.empty)
       // } else {
          val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1, Duration.ofMinutes(timeout))
          val splitPath = s"$path{${schema.fieldNames.mkString(",")}}[#${partition.startRowInclusive}:#${partition.endRowExclusive}]"
          val request = new ReadTable(splitPath, SparkRowDeserializer.getOrCreate(schema, filters))
          val reader = client.readTable(request).join()
          readTableIterator(reader)
       // }

      case _ => throw new IllegalArgumentException("Unexpected partition class")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1, Duration.ofMinutes(timeout))
    val rowCount = client.getNode(s"$path/@row_count").join().longValue()
    val builder = Array.newBuilder[Partition]
    for {
      index <- 0 until partitions
    } {
      builder += YtPartition(index, index * rowCount / partitions, (index + 1) * rowCount / partitions)
    }

    builder.result()
  }

  private def readTable(reader: TableReader[Row]): Seq[Row] = {
    import scala.collection.JavaConverters._
    val builder = Seq.newBuilder[Row]
    while (reader.canRead) {
      var chunk = reader.read()
      while (chunk != null) {
        chunk.iterator().asScala.foreach(row => builder += row)
        chunk = reader.read()
      }
      reader.readyEvent().join()
    }
    builder.result()
  }

  private def readTableIterator(reader: TableReader[Row]): Iterator[Row] = {
    new TableIterator(reader)
  }
}
