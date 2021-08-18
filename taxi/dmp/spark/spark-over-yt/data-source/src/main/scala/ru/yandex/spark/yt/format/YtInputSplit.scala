package ru.yandex.spark.yt.format

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.cypress.YPath

case class YtInputSplit(file: YtPartitionedFile, schema: StructType) extends InputSplit {
  override def getLength: Long = file.endRow - file.beginRow

  override def getLocations: Array[String] = Array.empty

  private val originalFieldNames = schema.fields.map(x => s""""${x.metadata.getString("original_name")}"""")
  private val basePath: YPath =
    YPath.simple(s"/${file.path}{${originalFieldNames.mkString(",")}}")

  def ytPath: YPath = {
    if (file.isDynamic) {
      import ru.yandex.spark.yt.serializers.PivotKeysConverter.toRangeLimit
      basePath.withRange(toRangeLimit(file.beginKey, file.keyColumns), toRangeLimit(file.endKey, file.keyColumns))
    } else {
      basePath.withRange(file.beginRow, file.endRow)
    }
  }
}
