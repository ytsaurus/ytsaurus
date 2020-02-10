package ru.yandex.spark.yt

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.fs.GlobalTableSettings

object PythonUtils {
  def setPathFilesCount(path: String, filesCount: Int): Unit = {
    GlobalTableSettings.setFilesCount(path, filesCount)
  }

  def schemaHint(dataFrameReader: DataFrameReader, schema: StructType): DataFrameReader = {
    dataFrameReader.schemaHint(schema)
  }
}
