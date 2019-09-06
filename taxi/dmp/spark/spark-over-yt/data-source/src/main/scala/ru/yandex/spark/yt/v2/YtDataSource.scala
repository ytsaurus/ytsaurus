package ru.yandex.spark.yt.v2

import java.util
import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.DefaultSourceParameters

class YtDataSource extends DataSourceV2 with ReadSupport with WriteSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = ???

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = ???
}
