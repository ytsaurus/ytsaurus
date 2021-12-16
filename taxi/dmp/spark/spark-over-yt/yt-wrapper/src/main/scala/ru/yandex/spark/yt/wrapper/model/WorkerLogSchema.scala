package ru.yandex.spark.yt.wrapper.model

import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.Key._
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

object WorkerLogSchema {
  object Key {
    val APP_DRIVER = "appDriver"
    val EXEC_ID = "execId"
    val STREAM = "stream"
    val ROW_ID = "rowId"
    val LOGGER_NAME = "loggerName"
    val TS = "timestamp"
    val DATE = "date"
    val LEVEL = "level"
    val SOURCE_HOST = "sourceHost"
    val FILE = "file"
    val LINE_NUMBER = "lineNumber"
    val THREAD = "thread"
    val MESSAGE = "message"
    val EXCEPTION_CLASS = "exceptionClass"
    val EXCEPTION_MESSAGE = "exceptionMessage"
    val STACK = "stack"

    val TABLE_NAME = "tableName"
    val LENGTH = "length"
  }

  val schema: TableSchema = TableSchema.builder()
    .addKey(APP_DRIVER, ColumnValueType.STRING)
    .addKey(EXEC_ID, ColumnValueType.STRING)
    .addKey(STREAM, ColumnValueType.STRING)
    .addKey(ROW_ID, ColumnValueType.INT64)
    .addKey(DATE, ColumnValueType.STRING)
    .addKey(LOGGER_NAME, ColumnValueType.STRING)
    .addKey(LEVEL, ColumnValueType.STRING)
    .addValue(SOURCE_HOST, ColumnValueType.STRING)
    .addValue(FILE, ColumnValueType.STRING)
    .addValue(LINE_NUMBER, ColumnValueType.STRING)
    .addValue(THREAD, ColumnValueType.STRING)
    .addValue(MESSAGE, ColumnValueType.STRING)
    .addValue(EXCEPTION_CLASS, ColumnValueType.STRING)
    .addValue(EXCEPTION_MESSAGE, ColumnValueType.STRING)
    .addValue(STACK, ColumnValueType.STRING)
    .setUniqueKeys(false)
    .build()

  val metaSchema: TableSchema = new TableSchema.Builder()
    .addKey(APP_DRIVER, ColumnValueType.STRING)
    .addKey(EXEC_ID, ColumnValueType.STRING)
    .addKey(STREAM, ColumnValueType.STRING)
    .addValue(TABLE_NAME, ColumnValueType.STRING)
    .addValue(LENGTH, ColumnValueType.INT64)
    .build()

  def getMetaPath(workerLogTablesPath: String): String = {
    s"$workerLogTablesPath/meta"
  }
}

