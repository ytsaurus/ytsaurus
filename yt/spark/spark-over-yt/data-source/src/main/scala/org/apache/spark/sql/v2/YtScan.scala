package org.apache.spark.sql.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Statistics, SupportsReportPartitioning}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.YtFilePartition.tryGetKeyPartitions
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import tech.ytsaurus.spyt.common.utils.SegmentSet
import tech.ytsaurus.spyt.format.conf.{FilterPushdownConfig, KeyPartitioningConfig, YtTableSparkSettings}
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.logger.YtDynTableLoggerConfig

import java.util.{Locale, OptionalLong}
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
                  pushedFilters: Seq[Filter] = Nil,
                  keyPartitionsHint: Option[Seq[FilePartition]] = None) extends FileScan
  with SupportsReportPartitioning with Logging {
  private val filterPushdownConf = FilterPushdownConfig(sparkSession)
  private val keyPartitioningConf = KeyPartitioningConfig(sparkSession)

  def supportsKeyPartitioning: Boolean = {
    keyPartitionsHint.isDefined
  }

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val keyPartitionedOptions = Map(YtTableSparkSettings.KeyPartitioned.name -> supportsKeyPartitioning.toString)
    YtPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema,
      options.asScala.toMap ++ keyPartitionedOptions,
      pushedFilterSegments, filterPushdownConf, YtDynTableLoggerConfig.fromSpark(sparkSession)
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
      ", filter pushdown enabled: " + filterPushdownConf.enabled +
      ", key partitioned: " + supportsKeyPartitioning
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

  // for tests
  private[v2] def getPartitions: Seq[FilePartition] = partitions

  private def tryGetKeyPartitioning(columns: Option[Seq[String]] = None): Option[Seq[FilePartition]] = {
    val splitFiles = preparePartitioning()
    tryGetKeyPartitions(sparkSession, splitFiles, readDataSchema, keyPartitioningConf, columns)
  }

  private def addKeyPartitioningHint(partitions: Seq[FilePartition]): YtScan = {
    copy(keyPartitionsHint = Some(partitions))
  }

  def tryKeyPartitioning(columns: Option[Seq[String]] = None): Option[YtScan] = {
    tryGetKeyPartitioning(columns).map(addKeyPartitioningHint)
  }

  override protected lazy val partitions: Seq[FilePartition] = {
    keyPartitionsHint.getOrElse {
      val splitFiles = preparePartitioning()
      YtFilePartition.getFilePartitions(splitFiles)
    }
  }

  private def preparePartitioning(): Seq[PartitionedFile] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, maybeReadParallelism)
    getSplitFiles(selectedPartitions, maxSplitBytes)
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
    lazy val partitionValueProject = GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
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
          partitionValues = partitionValues,
          readDataSchema = Some(readDataSchema)
        )
      }.toArray.sorted(YtFilePartition.partitionedFilesOrdering)
    }
  }

  override def outputPartitioning(): Partitioning = new Partitioning {
    override def numPartitions(): Int = partitions.length

    override def satisfy(distribution: Distribution): Boolean = false
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override val sizeInBytes: OptionalLong = OptionalLong.of(fileIndex.sizeInBytes)

    override val numRows: OptionalLong = {
      val rowCounts = fileIndex.allFiles().map { status =>
        YtHadoopPath.fromPath(status.getPath) match {
          case yp: YtHadoopPath => Some(yp.meta.rowCount)
          case _ => None
        }
      }
      if (rowCounts.forall(_.isDefined)) {
        OptionalLong.of(rowCounts.map(_.get).sum)
      } else {
        OptionalLong.empty()
      }
    }
  }
}

object YtScan {
  type ScanDescription = (YtScan, Seq[String])

  def trySyncKeyPartitioning(leftScanDescO: Option[ScanDescription], rightScanDescO: Option[ScanDescription]
                            ): (Option[ScanDescription], Option[ScanDescription]) = {
    def singleKeyPartitioning(scanDescO: Option[ScanDescription]): Option[ScanDescription] = {
      scanDescO.flatMap { case (scan, vars) => scan.tryKeyPartitioning(Some(vars)).map((_, vars)) }
    }
    (leftScanDescO, rightScanDescO) match {
      case (Some(leftDesc), Some(rightDesc)) =>
        trySyncKeyPartitioning(leftDesc, rightDesc)
      case _ =>
        (singleKeyPartitioning(leftScanDescO), singleKeyPartitioning(rightScanDescO))
    }
  }

  def trySyncKeyPartitioning(leftYtScanDesc: ScanDescription, rightYtScanDesc: ScanDescription
                            ): (Option[ScanDescription], Option[ScanDescription]) = {
    val (leftYtScan, leftVars) = leftYtScanDesc
    val (rightYtScan, rightVars) = rightYtScanDesc
    val leftPartitioningO = leftYtScan.tryGetKeyPartitioning(Some(leftVars))
    val rightPartitioningO = rightYtScan.tryGetKeyPartitioning(Some(rightVars))
    val (leftPartitioning, rightPartitioning) = (leftPartitioningO, rightPartitioningO) match {
      case (Some(leftPartitioning), Some(rightPartitioning)) =>
        val leftPivots = YtFilePartition.getPivotFromHintFiles(leftVars, leftPartitioning)
        val rightPivots = YtFilePartition.getPivotFromHintFiles(rightVars, rightPartitioning)
        val newLeftPartitioning = YtFilePartition.addPivots(leftPartitioning, leftVars, rightPivots)
        val newRightPartitioning = YtFilePartition.addPivots(rightPartitioning, rightVars, leftPivots)
        (Some(newLeftPartitioning), Some(newRightPartitioning))
      case p =>
        p
    }
    (
      leftPartitioning.map(leftYtScan.addKeyPartitioningHint).map((_, leftVars)),
      rightPartitioning.map(rightYtScan.addKeyPartitioningHint).map((_, rightVars))
    )
  }
}
