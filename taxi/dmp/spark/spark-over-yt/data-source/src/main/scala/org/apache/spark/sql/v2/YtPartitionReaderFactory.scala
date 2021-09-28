package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, YtVectorizedReader}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.Read.VectorizedCapacity
import ru.yandex.spark.yt.format.{YtInputSplit, YtPartitionedFile}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.serializers.InternalRowDeserializer
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

case class YtPartitionReaderFactory(sqlConf: SQLConf,
                                    broadcastedConf: Broadcast[SerializableConfiguration],
                                    dataSchema: StructType,
                                    readDataSchema: StructType,
                                    partitionSchema: StructType,
                                    options: Map[String, String]) extends FilePartitionReaderFactory with Logging {
  private val unsupportedTypes: Set[DataType] = Set(DateType, TimestampType)

  private val resultSchema = StructType(partitionSchema.fields ++ readDataSchema.fields)
  private val ytClientConf = ytClientConfiguration(sqlConf)
  private val arrowEnabled: Boolean = {
    import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration => SparkSettings, YtTableSparkSettings => TableSettings}
    import ru.yandex.spark.yt.fs.conf._
    options.ytConf(TableSettings.ArrowEnabled) && sqlConf.ytConf(SparkSettings.Read.ArrowEnabled)
  }
  private val readBatch: Boolean = {
    import ru.yandex.spark.yt.format.conf.{YtTableSparkSettings => TableSettings}
    val optimizedForScan = options.get(TableSettings.OptimizedForScan.name).exists(_.toBoolean)
    (optimizedForScan && arrowEnabled && arrowSchemaSupported(readDataSchema)) || readDataSchema.isEmpty
  }
  private val returnBatch: Boolean = {
    readBatch && sqlConf.wholeStageEnabled &&
      resultSchema.length <= sqlConf.wholeStageMaxNumFields &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
  }
  private val batchMaxSize = sqlConf.ytConf(VectorizedCapacity)


  override def supportColumnarReads(partition: InputPartition): Boolean = {
    returnBatch
  }

  def arrowSchemaSupported(dataSchema: StructType): Boolean = {
    dataSchema.fields.forall(f => !unsupportedTypes.contains(f.dataType))
  }

  private def buildLockedSplitReader[T](file: PartitionedFile)
                                       (splitReader: (YtInputSplit, Option[ApiServiceTransaction]) => PartitionReader[T])
                                       (implicit yt: CompoundClient): PartitionReader[T] = {
    file match {
      case ypf: YtPartitionedFile =>
        val split = createSplit(ypf)
        splitReader(split, None)
      case _ =>
        throw new IllegalArgumentException(s"Partitions of type ${file.getClass.getSimpleName} are not supported")
    }
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConf)
    buildLockedSplitReader(file) { case (split, transaction) =>
      val reader = if (readBatch) {
        createVectorizedReader(split, returnBatch = false)
      } else {
        createRowBaseReader(split)
      }

      val fileReader = new PartitionReader[InternalRow] {
        override def next(): Boolean = reader.nextKeyValue()

        override def get(): InternalRow = reader.getCurrentValue.asInstanceOf[InternalRow]

        override def close(): Unit = {
          transaction.foreach(_.commit().join())
          reader.close()
        }
      }

      new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
        partitionSchema, split.file.partitionValues)
    }
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConf)
    buildLockedSplitReader(file) { case (split, transaction) =>
      val vectorizedReader = createVectorizedReader(split, returnBatch = true)

      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = vectorizedReader.nextKeyValue()

        override def get(): ColumnarBatch =
          vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]

        override def close(): Unit = {
          transaction.foreach(_.commit().join())
          vectorizedReader.close()
        }
      }
    }
  }

  private def createSplit(file: YtPartitionedFile): YtInputSplit = {
    val log = LoggerFactory.getLogger(getClass)
    val split = YtInputSplit(file, resultSchema)

    log.info(s"Reading ${split.ytPath}, " +
      s"read batch: $readBatch, return batch: $returnBatch, arrowEnabled: $arrowEnabled")

    split
  }

  private def createRowBaseReader(split: YtInputSplit)
                                 (implicit yt: CompoundClient): RecordReader[Void, InternalRow] = {
    val iter = YtWrapper.readTable(
      split.ytPath,
      InternalRowDeserializer.getOrCreate(resultSchema),
      ytClientConf.timeout
    )
    val unsafeProjection = UnsafeProjection.create(resultSchema)

    new RecordReader[Void, InternalRow] {
      private var current: InternalRow = _

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

      override def nextKeyValue(): Boolean = {
        if (iter.hasNext) {
          current = unsafeProjection.apply(iter.next())
          true
        } else false
      }

      override def getCurrentKey: Void = {
        null
      }

      override def getCurrentValue: InternalRow = {
        current
      }

      override def getProgress: Float = 0.0f

      override def close(): Unit = {
        iter.close()
      }
    }
  }

  private def createVectorizedReader(split: YtInputSplit,
                                     returnBatch: Boolean)
                                    (implicit yt: CompoundClient): YtVectorizedReader = {
    new YtVectorizedReader(
      split = split,
      batchMaxSize = batchMaxSize,
      returnBatch = returnBatch,
      arrowEnabled = arrowEnabled,
      timeout = ytClientConf.timeout
    )
  }

}
