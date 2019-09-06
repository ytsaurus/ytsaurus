package ru.yandex.spark.yt

import java.time.Duration

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import ru.yandex.yt.ytclient.proxy.TableWriter
import ru.yandex.yt.ytclient.proxy.request.WriteTable
import ru.yandex.yt.ytclient.rpc.RpcCredentials

class YtRelation(@transient val sqlContext: SQLContext,
                 parameters: DefaultSourceParameters,
                 schemaHint: Option[StructType]) extends BaseRelation with PrunedFilteredScan with InsertableRelation {
  private val log = Logger.getLogger(getClass)

  import parameters._

  lazy val schema: StructType = {
    if (isSchemaFull && schemaHint.nonEmpty) {
      schemaHint.get
    } else {
      val timeout = sqlContext.getConf("spark.yt.timeout.minutes").toLong
      val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1, Duration.ofMinutes(timeout))
      val schemaTree = client.getNode(s"$path/@schema").join()
      SchemaConverter.sparkSchema(schemaTree, schemaHint)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val defaultPartitions = sqlContext.getConf("spark.sql.shuffle.partitions").toInt
    new YtRDD(sqlContext.sparkContext, path, proxy, user, token,
      partitions.getOrElse(defaultPartitions), StructType(requiredColumns.map(schema(_))), filters)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val params = parameters
    val s = schema
    val timeout = sqlContext.getConf("spark.yt.timeout.minutes").toLong
    val writeStep = sqlContext.getConf("spark.yt.write.step").toInt

    data.foreachPartition { rows: Iterator[Row] =>
      if (rows.hasNext) {
        val client = YtClientProvider.ytClient(params.proxy, new RpcCredentials(params.user, params.token), 1, Duration.ofMinutes(timeout))
        val serializer = new YsonRowConverter(s)
        val request = new WriteTable[Row](s"""<"append"=%true>${params.path}""", serializer)
        var writer = Option.empty[TableWriter[Row]]

        try {
          writer = Some(client.writeTable(request).join())
          rows.grouped(writeStep).foreach { chunk =>
            serializer.writeRows(writer.get, chunk)
          }
        } finally {
          writer.foreach(_.close().join())
        }
      }
    }
  }


}
