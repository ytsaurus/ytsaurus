package org.apache.spark.sql.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import ru.yandex.spark.yt.fs.YtDynamicPath

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
                  dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
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
      dataSchema, readDataSchema, readPartitionSchema, options.asScala.toMap)
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: YtScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description()
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

  override protected def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = YtFilePartition.maxSplitBytes(sparkSession, selectedPartitions, maybeReadParallelism)
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
    val splitFiles = selectedPartitions.flatMap { partition =>
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
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (splitFiles.length == 1) {
      val path = new Path(splitFiles.head.filePath)
      if (!isSplitable(path) && splitFiles.head.length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }
}

