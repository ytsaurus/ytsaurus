package ru.yandex.spark.yt.format

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

class YtPartitionedFile(val path: String,
                        val beginKey: Array[Byte],
                        val endKey: Array[Byte],
                        val beginRow: Long,
                        val endRow: Long,
                        val isDynamic: Boolean,
                        val keyColumns: Seq[String])
  extends PartitionedFile(partitionValues = YtPartitionedFile.emptyInternalRow,
    filePath = path,
    start = 0,
    length = 1)

object YtPartitionedFile {
  private val emptyInternalRow = new GenericInternalRow(new Array[Any](0))
}