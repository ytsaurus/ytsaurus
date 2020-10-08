package ru.yandex.spark.yt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonEncoder
import ru.yandex.spark.yt.fs.GlobalTableSettings
import ru.yandex.spark.yt.fs.conf.YtLogicalType

object PythonUtils {
  def setPathFilesCount(path: String, filesCount: Int): Unit = {
    GlobalTableSettings.setFilesCount(path, filesCount)
  }

  def schemaHint(dataFrameReader: DataFrameReader, schema: StructType): DataFrameReader = {
    dataFrameReader.schemaHint(schema)
  }

  def schemaHint[T](dataFrameWriter: DataFrameWriter[T],
                    schemaHint: java.util.HashMap[String, String]): DataFrameWriter[T] = {
    import scala.collection.JavaConverters._
    dataFrameWriter.schemaHint(schemaHint.asScala.toMap.mapValues(YtLogicalType.fromName))
  }

  def withYsonColumn(dataFrame: DataFrame, name: String, column: Column): DataFrame = {
    dataFrame.withYsonColumn(name, column)
  }

  def serializeColumnToYson(dataFrame: DataFrame, oldName: String, newName: String, skipNulls: Boolean): DataFrame = {
    val dataType = dataFrame.schema.fields(dataFrame.schema.fieldIndex(oldName)).dataType
    val broadcastDataType = dataFrame.sparkSession.sparkContext.broadcast(dataType)
    val columnToYsonUdf = dataType match {
      case _: StructType =>
        udf((row: Row) =>
          YsonEncoder.encode(row, broadcastDataType.value, skipNulls)
        )
    }
    dataFrame.withColumn(newName, columnToYsonUdf(col(oldName)))
  }
}
