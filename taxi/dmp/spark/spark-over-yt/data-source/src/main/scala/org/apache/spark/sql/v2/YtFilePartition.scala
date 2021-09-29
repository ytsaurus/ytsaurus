package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format.YtPartitionedFile
import ru.yandex.spark.yt.fs.conf.YT_MIN_PARTITION_BYTES
import ru.yandex.spark.yt.fs.{YtDynamicPath, YtPath, YtStaticPath}

import scala.collection.mutable.ArrayBuffer

object YtFilePartition {
  @transient private val log = LoggerFactory.getLogger(getClass)

  def maxSplitBytes(sparkSession: SparkSession,
                    selectedPartitions: Seq[PartitionDirectory],
                    maybeReadParallelism: Option[Int]): Long = {
    val defaultMaxSplitBytes =
      sparkSession.sessionState.conf.filesMaxPartitionBytes
    val minSplitBytes = org.apache.spark.network.util.JavaUtils.byteStringAs(
      sparkSession.sessionState.conf.getConfString(YT_MIN_PARTITION_BYTES, "1G"),
      ByteUnit.BYTE
    )

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = maybeReadParallelism
      .map { readParallelism =>
        totalBytes / readParallelism + 1
      }
      .getOrElse {
        Math.max(minSplitBytes, Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore)))
      }

    log.info(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes, " +
      s"default parallelism is $defaultParallelism, " +
      s"paths: ${selectedPartitions.flatMap(_.files.map(_.getPath.toString)).mkString(", ")}")

    maxSplitBytes
  }

  def getPartitionedFile(file: FileStatus,
                         offset: Long,
                         size: Long,
                         partitionValues: InternalRow): PartitionedFile = {
    YtPath.fromPath(file.getPath) match {
      case yp: YtDynamicPath =>
        YtPartitionedFile.dynamic(yp.toStringPath, yp.beginKey, yp.endKey, file.getLen,
          yp.keyColumns, file.getModificationTime)
      case p =>
        PartitionedFile(partitionValues, p.toUri.toString, offset, size, Array.empty)
    }
  }

  def getPartitionedFile(file: FileStatus,
                         path: YtStaticPath,
                         rowOffset: Long,
                         rowCount: Long,
                         byteSize: Long): PartitionedFile = {
    YtPartitionedFile.static(
      path.toStringPath,
      path.attrs.beginRow + rowOffset,
      path.attrs.beginRow + rowOffset + rowCount,
      byteSize,
      file.getModificationTime
    )
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
          val maxSplitRows = Math.max(1, Math.ceil(maxSplitBytes.toDouble / file.getLen * yp.rowCount).toLong)
          split(yp.rowCount, maxSplitRows) { case (offset, size) =>
            getPartitionedFile(file, yp, offset, size, size * file.getBlockSize)
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

  val partitionedFilesOrdering: Ordering[PartitionedFile] = {
    (x: PartitionedFile, y: PartitionedFile) => {
      (x, y) match {
        case (xYt: YtPartitionedFile, yYt: YtPartitionedFile)
          if xYt.path == yYt.path && !xYt.isDynamic && !yYt.isDynamic =>
          xYt.beginRow.compare(yYt.beginRow)
        case (xYt: YtPartitionedFile, yYt: YtPartitionedFile) =>
          xYt.path.compare(yYt.path)
        case (_: YtPartitionedFile, _) => -1
        case (_, _: YtPartitionedFile) => 1
        case (_, _) => y.length.compare(x.length)
      }
    }
  }

  def mergeFiles(files: Seq[PartitionedFile]): Array[PartitionedFile] = {
    val buffer = new ArrayBuffer[PartitionedFile]
    var currentFile: YtPartitionedFile = null

    def isMergeable(file: YtPartitionedFile): Boolean = {
      currentFile.path == file.path && currentFile.endRow == file.beginRow
    }

    def merge(file: YtPartitionedFile): YtPartitionedFile = {
      currentFile.copy(newEndRow = file.endRow)
    }

    files.foreach {
      case ytFile: YtPartitionedFile if !ytFile.isDynamic =>
        if (currentFile == null) {
          currentFile = ytFile
        } else {
          if (isMergeable(ytFile)) {
            currentFile = merge(ytFile)
          } else {
            buffer.append(currentFile)
            currentFile = ytFile
          }
        }
      case file =>
        if (currentFile != null) {
          buffer.append(currentFile)
          currentFile = null
        }
        buffer.append(file)
    }
    if (currentFile != null) {
      buffer.append(currentFile)
    }

    buffer.toArray
  }

  def getFilePartitions(sparkSession: SparkSession,
                        partitionedFiles: Seq[PartitionedFile],
                        maxSplitBytes: Long): Seq[FilePartition] = {
    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, mergeFiles(currentFiles))
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions
  }

}
