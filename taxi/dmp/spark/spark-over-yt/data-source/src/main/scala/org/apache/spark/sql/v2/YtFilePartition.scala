package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.common.utils.{ExpressionTransformer, PInfinity, TuplePoint}
import ru.yandex.spark.yt.format.conf.KeyPartitioningConfig
import ru.yandex.spark.yt.format.{YtInputSplit, YtPartitionedFile}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.conf.YT_MIN_PARTITION_BYTES
import ru.yandex.spark.yt.fs.path.YPathEnriched.ypath
import ru.yandex.spark.yt.fs.{YtDynamicPath, YtPath, YtStaticPath}
import ru.yandex.spark.yt.serializers.{InternalRowDeserializer, PivotKeysConverter, SchemaConverter}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.yt.ytclient.proxy.CompoundClient

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
          yp.keyColumns, file.getModificationTime, partitionValues)
      case p =>
        PartitionedFile(partitionValues, p.toUri.toString, offset, size, Array.empty)
    }
  }

  def getPartitionedFile(file: FileStatus,
                         path: YtStaticPath,
                         rowOffset: Long,
                         rowCount: Long,
                         byteSize: Long,
                         partitionValues: InternalRow): PartitionedFile = {
    YtPartitionedFile.static(
      path.toStringPath,
      path.attrs.beginRow + rowOffset,
      path.attrs.beginRow + rowOffset + rowCount,
      byteSize,
      file.getModificationTime,
      partitionValues
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
            getPartitionedFile(file, yp, offset, size, size * file.getBlockSize, partitionValues)
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

  def tryGetKeyPartitions(sparkSession: SparkSession, splitFiles: Seq[PartitionedFile], schema: StructType,
                          keyPartitioningConfig: KeyPartitioningConfig,
                          requiredKeysO: Option[Seq[String]] = None): Option[Seq[FilePartition]] = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(sparkSession.sessionState.conf))
    val keys = SchemaConverter.prefixKeys(schema)
    log.info(s"Partitioned table has keys ${keys.toString()}. " +
      s"Required keys: ${requiredKeysO.map(_.mkString(",")).getOrElse("-")}")
    if (keys.nonEmpty && keyPartitioningConfig.enabled && requiredKeysO.forall(keys.startsWith(_))) {
      val isSupportedFiles = splitFiles.forall {
        case file: YtPartitionedFile => !file.isDynamic
        case _ => false
      }
      if (isSupportedFiles) {
        log.info("Key partitioning supports all files")
        val ytSplitFiles = splitFiles.asInstanceOf[Seq[YtPartitionedFile]]
        if (ytSplitFiles.isEmpty) {
          log.info("Empty file list")
          Some(Seq.empty)
        } else {
          if (ytSplitFiles.forall(ytSplitFiles.head.filePath == _.filePath)) {
            val keyPartitioning = requiredKeysO match {
              case Some(requiredKeys) => getKeyPartitions(schema, requiredKeys, ytSplitFiles, keyPartitioningConfig)
              case None => collectFirstKeyPartitions(schema, keys, ytSplitFiles, keyPartitioningConfig)
            }
            keyPartitioning match {
              case Some(partitions) =>
                log.info(s"Used key partitioning for key set ${keys.toString()}")
                Some(partitions.indices.zip(partitions).map { case (i, x) => FilePartition(i, Array(x)) })
              case None =>
                log.info("Unsuccessful using of key partitioning")
                None
            }
          } else {
            // TODO support correct few tables reading
            log.info("Reading few tables try")
            None
          }
        }
      } else {
        log.info("Unsupported files found")
        None
      }
    } else {
      log.info("Key partitioning hasn't been tried")
      None
    }
  }

  private def collectFirstKeyPartitions(schema: StructType, keys: Seq[String], splitFiles: Seq[YtPartitionedFile],
                                        keyPartitioningConfig: KeyPartitioningConfig)
                              (implicit yt: CompoundClient): Option[Seq[PartitionedFile]] = {
    (1 to keys.size).collectFirst {
      Function.unlift {
        colCount =>
          getKeyPartitions(schema, keys.take(colCount), splitFiles, keyPartitioningConfig)
      }
    }
  }

  private def getKeyPartitions(schema: StructType, currentKeys: Seq[String],
                               splitFiles: Seq[YtPartitionedFile], keyPartitioningConfig: KeyPartitioningConfig)
                              (implicit yt: CompoundClient): Option[Seq[YtPartitionedFile]] = {
    log.info(currentKeys.length + " columns try for key partitioning. Key set: " + currentKeys)
    val keySchema = StructType(schema.fields.filter(f => currentKeys.contains(f.name)))
    if (keySchema.fields.map(_.name).toSeq != currentKeys) {
      log.error("Key partitioning schema is " + keySchema + ", but other keys (" + currentKeys + ") required")
      None
    } else {
      if (keySchema.fields.forall(f => ExpressionTransformer.isSupportedDataType(f.dataType))) { // always true
        log.info("All columns are supported for key partitioning")
        val pivotKeys = getPivotKeys(keySchema, currentKeys, splitFiles)
        log.info("Pivot keys: " + pivotKeys.mkString(", "))
        val filesGroupedByKey = seqGroupBy(pivotKeys.zip(splitFiles))
        if (filesGroupedByKey.forall { case (_, files) => files.length <= keyPartitioningConfig.unionLimit }) {
          log.info("Coalesced partitions satisfy union limit")
          Some(getFilesWithUniquePivots(currentKeys, filesGroupedByKey))
        } else {
          log.info("Coalesced partitions don't satisfy union limit")
          None
        }
      } else {
        log.info("Not all columns are supported for key partitioning")
        None
      }
    }
  }

  private[v2] def getFilesWithUniquePivots(keys: Seq[String],
                                       filesGroupedByPoint: Seq[(TuplePoint, Seq[YtPartitionedFile])]): Seq[YtPartitionedFile] = {
    val maxPoint = TuplePoint(Seq(PInfinity()))
    val (_, res) = filesGroupedByPoint
      .foldRight((maxPoint, Seq.empty[YtPartitionedFile])) {
        case ((curPoint, curFiles), (nextPoint, res)) =>
          (curPoint, putPivotKeysToFile(curFiles.head, keys, curPoint, nextPoint) +: res)
      }
    res
  }

  private[v2] def getPivotKeys(schema: StructType, keys: Seq[String], files: Seq[YtPartitionedFile])
                          (implicit yt: CompoundClient): Seq[TuplePoint] = {
    val filepath = files.head.filePath
    val basePath = ypath(new Path(filepath)).toYPath.withColumns(keys: _*)
    val pathWithRanges = files.foldLeft(basePath) {
      case (path, pf) => path.withRange(pf.beginRow, pf.beginRow + 1)
    }
    val tableIterator = YtWrapper.readTable(
      pathWithRanges,
      InternalRowDeserializer.getOrCreate(schema),
      reportBytesRead = _ => ()
    )
    try {
      getParsedRows(tableIterator, schema).sorted
    } finally {
      tableIterator.close()
    }
  }

  private def getParsedRows(iterator: Iterator[InternalRow], schema: StructType): Seq[TuplePoint] = {
    iterator.map {
      row =>
        TuplePoint(
          row
            .toSeq(schema)
            .map(ExpressionTransformer.parseOrderedLiteral(_).get)
        )
    }.toSeq
  }

  private def putPivotKeysToFile(file: YtPartitionedFile, keys: Seq[String],
                                 start: TuplePoint, end: TuplePoint): YtPartitionedFile = {
    file.copy(getByteKey(keys, start), getByteKey(keys, end), keys)
  }

  private def getByteKey(columns: Seq[String], key: TuplePoint): Array[Byte] = {
    PivotKeysConverter.toByteArray(columns.zip(key.points.map(YtInputSplit.prepareKey)).toMap)
  }

  private[v2] def seqGroupBy[K, V](valuesWithKeys: Seq[(K, V)]): Seq[(K, Seq[V])] = {
    valuesWithKeys.foldRight(Seq.empty[(K, Seq[V])]) {
      case ((curKey, curValue), acc) =>
        acc match {
          case (prevKey, prevValues) :: tail if prevKey == curKey =>
            (prevKey, curValue +: prevValues) +: tail
          case _ =>
            (curKey, Seq(curValue)) +: acc
        }
    }
  }
}
