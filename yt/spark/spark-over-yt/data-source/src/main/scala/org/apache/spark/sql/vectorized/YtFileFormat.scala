package org.apache.spark.sql.vectorized

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.v2.YtUtils.bytesReadReporter
import org.apache.spark.sql.v2.{YtReaderOptions, YtUtils}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.cypress.{RichYPath, YPath}
import tech.ytsaurus.spyt.format._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read._
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, SparkYtWriteConfiguration}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.logger.YtDynTableLoggerConfig
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.streaming.{YtStreamingSink, YtStreamingSource}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtFileFormat extends FileFormat with DataSourceRegister with StreamSourceProvider with StreamSinkProvider
  with Serializable {
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    YtUtils.inferSchema(sparkSession, options, files)
  }


  override def vectorTypes(requiredSchema: StructType,
                           partitionSchema: StructType,
                           sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.length)(classOf[ColumnVector].getName))
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    import tech.ytsaurus.spyt.fs.conf._
    val ytClientConf = ytClientConfiguration(hadoopConf)

    val sqlConf = sparkSession.sqlContext.conf
    val arrowEnabledValue = YtReaderOptions.arrowEnabled(options, sqlConf)
    val optimizedForScanValue = YtReaderOptions.optimizedForScan(options)
    val readBatch = YtReaderOptions.canReadBatch(requiredSchema, optimizedForScanValue, arrowEnabledValue)
    val returnBatch = readBatch && YtReaderOptions.supportBatch(requiredSchema, sqlConf)
    val filterPushdownConfig = FilterPushdownConfig(sparkSession)

    val batchMaxSize = hadoopConf.ytConf(VectorizedCapacity)
    val countOptimizationEnabled = hadoopConf.ytConf(CountOptimizationEnabled)

    val log = LoggerFactory.getLogger(getClass)
    log.info(s"Batch read enabled: $readBatch")
    log.info(s"Batch return enabled: $returnBatch")
    log.info(s"Optimized for scan: $optimizedForScanValue")
    log.info(s"Arrow enabled: $arrowEnabledValue")
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val ytLoggerConfig = YtDynTableLoggerConfig.fromSpark(sparkSession)

    {
      case ypf: YtPartitionedFile =>
        val log = LoggerFactory.getLogger(getClass)
        implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConf)
        val split = YtInputSplit(ypf, requiredSchema, filterPushdownConfig = filterPushdownConfig,
          ytLoggerConfig = ytLoggerConfig)
        log.info(s"Reading ${split.ytPathWithFilters}")
        if (readBatch) {
          val ytVectorizedReader = new YtVectorizedReader(
            split = split,
            batchMaxSize = batchMaxSize,
            returnBatch = returnBatch,
            arrowEnabled = arrowEnabledValue,
            optimizedForScan = optimizedForScanValue,
            timeout = ytClientConf.timeout,
            reportBytesRead = bytesReadReporter(broadcastedConf),
            countOptimizationEnabled = countOptimizationEnabled,
          )
          val iter = new RecordReaderIterator(ytVectorizedReader)
          Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
          if (!returnBatch) {
            val unsafeProjection = if (arrowEnabledValue) {
              ColumnarBatchRowUtils.unsafeProjection(requiredSchema)
            } else {
              UnsafeProjection.create(requiredSchema)
            }
            iter.asInstanceOf[Iterator[InternalRow]].map(unsafeProjection)
          } else {
            iter.asInstanceOf[Iterator[InternalRow]]
          }
        } else {
          val tableIterator = YtWrapper.readTable(
            split.ytPathWithFilters,
            InternalRowDeserializer.getOrCreate(requiredSchema),
            ytClientConf.timeout,
            None,
            bytesReadReporter(broadcastedConf)
          )
          val unsafeProjection = UnsafeProjection.create(requiredSchema)
          tableIterator.map(unsafeProjection(_))
        }
    }
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    YtOutputWriterFactory.create(
        SparkYtWriteConfiguration(sparkSession.sqlContext),
        ytClientConfiguration(sparkSession),
        options,
        dataSchema,
        job.getConfiguration
    )
  }

  override def shortName(): String = "yt"

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = true

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    YtReaderOptions.supportBatch(dataSchema, sparkSession.sqlContext.conf)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val ypath = RichYPath.fromString(caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH))
    (shortName(), YtUtils.getSchema(sqlContext.sparkSession, ypath, None, caseInsensitiveParameters))
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val consumerPath = caseInsensitiveParameters(YtUtils.Options.CONSUMER_PATH)
    val queuePath = caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH)
    val requiredSchema = schema.getOrElse {
      YtUtils.getSchema(sqlContext.sparkSession, YPath.simple(queuePath), None, caseInsensitiveParameters)
    }
    new YtStreamingSource(sqlContext, consumerPath, queuePath, requiredSchema, caseInsensitiveParameters)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    require(outputMode == OutputMode.Append(), "Only append mode is supported now")
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    val richQueuePath = RichYPath.fromString(caseInsensitiveParameters(YtUtils.Options.QUEUE_PATH))
    new YtStreamingSink(sqlContext, richQueuePath.justPath().toStableString, caseInsensitiveParameters)
  }
}
