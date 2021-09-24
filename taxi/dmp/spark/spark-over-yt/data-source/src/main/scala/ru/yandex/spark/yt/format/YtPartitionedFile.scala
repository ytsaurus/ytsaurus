package ru.yandex.spark.yt.format

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

class YtPartitionedFile(val path: String,
                        val beginKey: Array[Byte],
                        val endKey: Array[Byte],
                        val beginRow: Long,
                        val endRow: Long,
                        val byteSize: Long,
                        val isDynamic: Boolean,
                        val keyColumns: Seq[String],
                        val modificationTs: Long)
  extends PartitionedFile(
    partitionValues = YtPartitionedFile.emptyInternalRow,
    filePath = path,
    start = beginRow,
    length = byteSize
  ) {
  def copy(path: String): YtPartitionedFile = {
    new YtPartitionedFile(path, beginKey, endKey, beginRow, endRow, byteSize, isDynamic, keyColumns, modificationTs)
  }
}

object YtPartitionedFile {
  private val emptyInternalRow = new GenericInternalRow(new Array[Any](0))
}