package ru.yandex.spark.yt.v2

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.DefaultSourceParameters

class YtDataSourceReader(parameters: DefaultSourceParameters,
                         options: DataSourceOptions) extends DataSourceReader {
  override def readSchema(): StructType = ???

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}
