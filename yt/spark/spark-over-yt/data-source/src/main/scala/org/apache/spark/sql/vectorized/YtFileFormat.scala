package org.apache.spark.sql.vectorized

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.v2.YtUtils.bytesReadReporter
import org.apache.spark.sql.v2.{YtReaderOptions, YtUtils}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read._
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, SparkYtWriteConfiguration}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.YtDynamicPath
import tech.ytsaurus.spyt.logger.YtDynTableLoggerConfig
import tech.ytsaurus.spyt.serializers.InternalRowDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtFileFormat extends FileFormat with DataSourceRegister with Serializable {
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
            bytesReadReporter(broadcastedConf)
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

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    path match {
      case _: YtDynamicPath => false
      case _ => true
    }
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    YtReaderOptions.supportBatch(dataSchema, sparkSession.sqlContext.conf)
  }
}
