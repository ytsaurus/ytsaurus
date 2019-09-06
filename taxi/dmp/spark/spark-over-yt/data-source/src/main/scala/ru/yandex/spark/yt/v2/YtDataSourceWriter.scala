package ru.yandex.spark.yt.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

class YtDataSourceWriter extends DataSourceWriter {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = ???

  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
