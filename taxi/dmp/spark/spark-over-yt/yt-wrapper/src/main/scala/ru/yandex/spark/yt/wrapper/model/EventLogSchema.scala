package ru.yandex.spark.yt.wrapper.model

import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

object EventLogSchema {
  val schema: TableSchema = TableSchema.builder()
    .addKey("id", ColumnValueType.STRING)
    .addKey("order", ColumnValueType.INT64)
    .addValue("log", ColumnValueType.STRING).build()

  val metaSchema: TableSchema = TableSchema.builder()
    .addKey("file_name", ColumnValueType.STRING)
    .addValue("id", ColumnValueType.STRING)
    .addValue("meta", ColumnValueType.ANY).build()
}
