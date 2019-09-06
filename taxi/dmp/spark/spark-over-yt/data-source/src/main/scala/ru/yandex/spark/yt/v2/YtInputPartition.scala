package ru.yandex.spark.yt.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class YtInputPartition extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = ???
}
