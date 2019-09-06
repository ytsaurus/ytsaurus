package ru.yandex.spark.yt.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

class YtInputPartitionReader extends InputPartitionReader[InternalRow] {
  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}
