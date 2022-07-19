package org.apache.spark.sql.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, SupportsReportPartitioning}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.YtFilePartition.getFilePartition
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import ru.yandex.spark.yt.common.utils._
import ru.yandex.spark.yt.format.conf.{FilterPushdownConfig, KeyPartitioningConfig}
import ru.yandex.spark.yt.fs.YtDynamicPath
import ru.yandex.spark.yt.logger.YtDynTableLoggerConfig

import java.util.Locale
import scala.collection.JavaConverters._

case class YtScan(sparkSession: SparkSession,
                  hadoopConf: Configuration,
                  fileIndex: PartitioningAwareFileIndex,
                  dataSchema: StructType,
                  readDataSchema: StructType,
                  readPartitionSchema: StructType,
                  options: CaseInsensitiveStringMap,
                  partitionFilters: Seq[Expression] = Seq.empty,
                  dataFilters: Seq[Expression] = Seq.empty,
                  pushedFilterSegments: SegmentSet = SegmentSet(),
                  pushedFilters: Seq[Filter] = Nil) extends FileScan with SupportsReportPartitioning with Logging {
  private val filterPushdownConf = FilterPushdownConfig(sparkSession)
  private val keyPartitioningConf = KeyPartitioningConfig(sparkSession)

  override def isSplitable(path: Path): Boolean = {
    path match {
      case _: YtDynamicPath => false
      case _ => true
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    YtPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, options.asScala.toMap, pushedFilterSegments, filterPushdownConf,
      YtDynTableLoggerConfig.fromSpark(sparkSession)
    )
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: YtScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() +
      ", PushedFilters: " + seqToString(pushedFilters) +
      ", filter pushdown enabled: " + filterPushdownConf.enabled
  }

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  private val maybeReadParallelism = Option(options.get("readParallelism")).map(_.toInt)

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  private var knownNumPartitions: Int = _

  // for tests
  private[v2] def getPartitions: Seq[FilePartition] = partitions

  override protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, maybeReadParallelism)
    val splitFiles = getSplitFiles(selectedPartitions, maxSplitBytes)

    if (splitFiles.length == 1) {
      val path = new Path(splitFiles.head.filePath)
      if (!isSplitable(path) && splitFiles.head.length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    val partitions = getFilePartition(sparkSession, splitFiles, readDataSchema, keyPartitioningConf, maxSplitBytes)
    knownNumPartitions = partitions.length
    partitions
  }

  private def getSplitFiles(selectedPartitions: Seq[PartitionDirectory], maxSplitBytes: Long): Seq[PartitionedFile] = {
    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(
        normalizeName(readField.name),
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        YtFilePartition.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sorted(YtFilePartition.partitionedFilesOrdering)
    }
  }

  override def outputPartitioning(): Partitioning = new Partitioning {
    override def numPartitions(): Int = knownNumPartitions

    override def satisfy(distribution: Distribution): Boolean = false
  }
}
