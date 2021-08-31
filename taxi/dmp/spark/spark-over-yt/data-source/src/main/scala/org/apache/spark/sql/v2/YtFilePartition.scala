package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format.YtPartitionedFile
import ru.yandex.spark.yt.fs.{YtDynamicPath, YtPath, YtStaticPath}

object YtFilePartition {
  @transient private val log = LoggerFactory.getLogger(getClass)

  def maxSplitBytes(sparkSession: SparkSession,
                    selectedPartitions: Seq[PartitionDirectory],
                    maybeReadParallelism: Option[Int]): Long = {
    val defaultMaxSplitBytes =
      sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = maybeReadParallelism
      .map { readParallelism =>
        totalBytes / readParallelism + 1
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
        new YtPartitionedFile(yp.stringPath, yp.beginKey, yp.endKey, 0, 1, file.getLen, isDynamic = true, yp.keyColumns)
      case p =>
        PartitionedFile(partitionValues, p.toUri.toString, offset, size, Array.empty)
    }
  }

  def getPartitionedFile(path: YtStaticPath,
                         rowOffset: Long,
                         rowCount: Long,
                         byteSize: Long): PartitionedFile = {
    new YtPartitionedFile(path.stringPath, Array.empty, Array.empty, path.beginRow + rowOffset,
      path.beginRow + rowOffset + rowCount, byteSize, isDynamic = false, Nil)
  }

  def splitFiles(sparkSession: SparkSession,
                 file: FileStatus,
                 filePath: Path,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    def split(length: Long, splitSize: Long)
             (f: (Long, Long) => PartitionedFile): Seq[PartitionedFile] = {
      (0L until length by splitSize).map { offset =>
        val remaining = length - offset
        val size = if (remaining > splitSize) splitSize else remaining
        f(offset, size)
      }
    }

    if (isSplitable) {
      YtPath.fromPath(file.getPath) match {
        case yp: YtStaticPath =>
          val maxSplitRows = Math.max(1, Math.ceil(maxSplitBytes.toDouble / file.getLen  * yp.rowCount).toLong)
          split(yp.rowCount, maxSplitRows) { case (offset, size) =>
            getPartitionedFile(yp, offset, size, size * file.getBlockSize)
          }
        case _ =>
          split(file.getLen, maxSplitBytes) { case (offset, size) =>
            getPartitionedFile(file, offset, size, partitionValues)
          }
      }

    } else {
      Seq(getPartitionedFile(file, 0, file.getLen, partitionValues))
    }
  }

}
