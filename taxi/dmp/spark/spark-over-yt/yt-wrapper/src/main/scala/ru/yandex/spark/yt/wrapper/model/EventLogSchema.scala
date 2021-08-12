package ru.yandex.spark.yt.wrapper.model

import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

object EventLogSchema {
  object Key {
    val ID = "id"
    val ORDER = "order"
    val LOG = "log"

    val FILENAME = "file_name"
    val META = "meta"

    val ROW_SIZE = "rowSize"
    val BLOCKS_CNT = "blocksCnt"
    val LENGTH = "length"
    val MODIFICATION_TS = "modificationTs"
  }

  val schema: TableSchema = TableSchema.builder()
    .addKey(ID, ColumnValueType.STRING)
    .addKey(ORDER, ColumnValueType.INT64)
    .addValue(LOG, ColumnValueType.STRING).build()

  val metaSchema: TableSchema = TableSchema.builder()
    .addKey(FILENAME, ColumnValueType.STRING)
    .addValue(ID, ColumnValueType.STRING)
    .addValue(META, ColumnValueType.ANY).build()
}
