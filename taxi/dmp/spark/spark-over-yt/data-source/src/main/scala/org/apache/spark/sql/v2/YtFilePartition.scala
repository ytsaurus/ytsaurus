package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import ru.yandex.spark.yt.format.YtPartitionedFile
import ru.yandex.spark.yt.fs.{YtDynamicPath, YtFileStatus, YtPath, YtStaticPath}

object YtFilePartition {
  @transient private val log = Logger.getLogger(getClass)

  def maxSplitBytes(sparkSession: SparkSession,
                    selectedPartitions: Seq[PartitionDirectory],
                    optionMaxSplitBytes: Option[Long]): Long = {
    val defaultMaxSplitBytes =
      sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = optionMaxSplitBytes
      .orElse {
        selectedPartitions.flatMap { part =>
          part.files.flatMap {
            case f: YtFileStatus => Some(f.avgChunkSize)
            case f => YtStaticPath.fromPath(f.getPath).map(_.rowCount)
          }.reduceOption(_ max _)
        }.reduceOption(_ max _)
      }
      .getOrElse {
        Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
      }

    log.info(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    maxSplitBytes
  }

  def getPartitionedFile(file: FileStatus,
                         offset: Long,
                         size: Long,
                         partitionValues: InternalRow): PartitionedFile = {
    YtPath.fromPath(file.getPath) match {
      case yp: YtDynamicPath =>
        new YtPartitionedFile(yp.stringPath, yp.beginKey, yp.endKey, 0, 1, isDynamic = true, yp.keyColumns)
      case yp: YtStaticPath =>
        new YtPartitionedFile(yp.stringPath, Array.empty, Array.empty, yp.beginRow + offset,
          yp.beginRow + offset + size, isDynamic = false, Nil)
      case p => PartitionedFile(partitionValues, p.toUri.toString, offset, size, Array.empty)
    }
  }

  def splitFiles(sparkSession: SparkSession,
                 file: FileStatus,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (isSplitable) {
      (0L until file.getLen by maxSplitBytes).map { offset =>
        val remaining = file.getLen - offset
        val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
        getPartitionedFile(file, offset, size, partitionValues)
      }
    } else {
      Seq(getPartitionedFile(file, 0, file.getLen, partitionValues))
    }
  }

}
