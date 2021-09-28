package ru.yandex.spark.yt.format

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

class YtPartitionedFile(val path: String,
                        val beginKey: Array[Byte],
                        val endKey: Array[Byte],
                        val beginRow: Long,
                        val endRow: Long,
                        val byteLength: Long,
                        val isDynamic: Boolean,
                        val keyColumns: Seq[String],
                        val modificationTs: Long)
  extends PartitionedFile(
    partitionValues = YtPartitionedFile.emptyInternalRow,
    filePath = path,
    start = beginRow,
    length = byteLength
  ) {
  def copy(newEndRow: Long): YtPartitionedFile = {
    new YtPartitionedFile(path, beginKey, endKey, beginRow, newEndRow, byteLength, isDynamic, keyColumns, modificationTs)
  }
}

object YtPartitionedFile {
  val emptyInternalRow = new GenericInternalRow(new Array[Any](0))

  def static(path: String, beginRow: Long, endRow: Long,
             byteLength: Long, modificationTs: Long): YtPartitionedFile = {
    new YtPartitionedFile(path, Array.empty, Array.empty, beginRow,
      endRow, byteLength, isDynamic = false, Nil, modificationTs)
  }

  def dynamic(path: String, beginKey: Array[Byte], endKey: Array[Byte],
              byteLength: Long, keyColumns: Seq[String], modificationTs: Long): YtPartitionedFile = {
    new YtPartitionedFile(path, beginKey, endKey, 0, 1, byteLength,
      isDynamic = true, keyColumns, modificationTs)
  }
}