package org.apache.spark.sql.vectorized

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format._
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.Read._
import ru.yandex.spark.yt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.{YtClientProvider, YtPath}
import ru.yandex.spark.yt.serializers.{InternalRowDeserializer, SchemaConverter}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

class YtFileFormat extends FileFormat with DataSourceRegister with Serializable {
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    files.headOption.map { fileStatus =>
      val schemaHint = SchemaConverter.schemaHint(options)
      implicit val client: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sparkSession))
      val path = fileStatus.getPath match {
        case ytPath: YtPath => ytPath.stringPath
        case p => YtPath.basePath(p)
      }
      val schemaTree = YtWrapper.attribute(path, "schema")
      SchemaConverter.sparkSchema(schemaTree, schemaHint)
    }
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
    import ru.yandex.spark.yt.fs.conf._
    val ytClientConf = ytClientConfiguration(hadoopConf)

    val arrowEnabledValue = arrowEnabled(options, hadoopConf)
    val readBatch = canReadBatch(requiredSchema, options, hadoopConf)
    val returnBatch = supportBatch(sparkSession, requiredSchema, options)

    val batchMaxSize = hadoopConf.ytConf(VectorizedCapacity)

    val log = LoggerFactory.getLogger(getClass)
    log.info(s"Batch read enabled: $readBatch")
    log.info(s"Batch return enabled: $returnBatch")
    log.info(s"Arrow enabled: $arrowEnabledValue")

    {
      case ypf: YtPartitionedFile =>
        val log = LoggerFactory.getLogger(getClass)
        implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConf)
        val split = YtInputSplit(ypf, requiredSchema)
        log.info(s"Reading ${split.ytPath}")
        log.info(s"Batch read enabled: $readBatch")
        log.info(s"Batch return enabled: $returnBatch")
        log.info(s"Arrow enabled: $arrowEnabledValue")
        if (readBatch) {
          val ytVectorizedReader = new YtVectorizedReader(
            split = split,
            batchMaxSize = batchMaxSize,
            returnBatch = returnBatch,
            arrowEnabled = arrowEnabledValue,
            timeout = ytClientConf.timeout
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
            split.ytPath,
            InternalRowDeserializer.getOrCreate(requiredSchema),
            ytClientConf.timeout
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
    SchemaConverter.checkSchema(dataSchema)

    val ytClientConf = ytClientConfiguration(sparkSession)
    val writeConfiguration = SparkYtWriteConfiguration(sparkSession.sqlContext)
    YtTableSparkSettings.serialize(options, dataSchema, job.getConfiguration)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        val transaction = YtOutputCommitter.getWriteTransaction(context.getConfiguration)
        new YtOutputWriter(path, dataSchema, ytClientConf, writeConfiguration, transaction, options)
      }
    }
  }

  override def shortName(): String = "yt"

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    import ru.yandex.spark.yt.format.conf.{YtTableSparkSettings => TableSettings}
    options.get(TableSettings.Dynamic.name).forall(!_.toBoolean)
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    false
  }

  def canReadBatch(dataSchema: StructType, options: Map[String, String], hadoopConf: Configuration): Boolean = {
    import ru.yandex.spark.yt.format.conf.{YtTableSparkSettings => TableSettings}
    val optimizedForScan = options.get(TableSettings.OptimizedForScan.name).exists(_.toBoolean)
    (optimizedForScan && arrowEnabled(options, hadoopConf) && arrowSchemaSupported(dataSchema)) || dataSchema.isEmpty
  }

  def arrowSchemaSupported(dataSchema: StructType): Boolean = {
    true
  }

  def arrowEnabled(options: Map[String, String], hadoopConf: Configuration): Boolean = {
    import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration => SparkSettings, YtTableSparkSettings => TableSettings}
    import ru.yandex.spark.yt.fs.conf._
    options.ytConf(TableSettings.ArrowEnabled) && hadoopConf.ytConf(SparkSettings.Read.ArrowEnabled)
  }

  def supportBatch(sparkSession: SparkSession, dataSchema: StructType, options: Map[String, String]): Boolean = {
    val conf = sparkSession.sessionState.conf
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    canReadBatch(dataSchema, options, hadoopConf) && conf.wholeStageEnabled &&
      dataSchema.length <= conf.wholeStageMaxNumFields &&
      dataSchema.forall(_.dataType.isInstanceOf[AtomicType])
  }
}
