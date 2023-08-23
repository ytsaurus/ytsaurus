package tech.ytsaurus.spyt.format

import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.ModifyRowsRequest
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{WriteSchemaHint, WriteTypeV3}
import tech.ytsaurus.spyt.fs.conf._
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider}

import scala.collection.JavaConverters._

class YtDynamicTableWriter(val path: String,
                           schema: StructType,
                           ytClientConfiguration: YtClientConfiguration,
                           wConfig: SparkYtWriteConfiguration,
                           options: Map[String, String]) extends OutputWriter {
  import YtDynamicTableWriter._

  private val schemaHint = options.ytConf(WriteSchemaHint)
  private val typeV3Format = options.ytConf(WriteTypeV3)

  private val tableSchema = SchemaConverter.tableSchema(schema, Unordered, schemaHint, typeV3Format)
  private var count = 0
  private var modifyRowsRequestBuilder: ModifyRowsRequest.Builder = null

  private implicit val ytClient: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration)

  initialize()

  override def write(row: InternalRow): Unit = {
    modifyRowsRequestBuilder.addInsert(row.toSeq(schema).map(toPrimitives).asJava)
    count += 1
    if (count == wConfig.dynBatchSize) {
      commitBatch()
    }
  }

  override def close(): Unit = {
    log.debug("Closing writer")
    if (count > 0) {
      commitBatch()
    }
  }

  private def initBatch(): Unit = {
    modifyRowsRequestBuilder = ModifyRowsRequest.builder().setPath(YtWrapper.formatPath(path)).setSchema(tableSchema)
    count = 0
  }

  private def commitBatch(): Unit = {

    log.debug(s"Batch size: ${wConfig.dynBatchSize}, actual batch size: ${count}")
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      val request: ModifyRowsRequest = modifyRowsRequestBuilder.build()
      val transaction = YtWrapper.createTransaction(parent = None, timeout = wConfig.timeout, sticky = true)

      transaction.modifyRows(request).join()
      transaction.commit().join()
    }
    initBatch()
  }

  private def initialize(): Unit = {
    log.debug(s"[${Thread.currentThread().getName}] Creating new YtDynamicTableWriter for path ${path}")
    initBatch()
    YtMetricsRegister.register()
  }
}

object YtDynamicTableWriter {
  private val log = LoggerFactory.getLogger(getClass)

  private def toPrimitives(value: Any): Any = value match {
      case v: UTF8String => v.getBytes
      case _ => value
  }
}
