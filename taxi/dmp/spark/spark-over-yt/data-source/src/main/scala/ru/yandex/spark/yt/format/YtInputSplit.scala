package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.spark.yt.fs.YPathEnriched.ypath

case class YtInputSplit(file: YtPartitionedFile,
                        schema: StructType) extends InputSplit {
  override def getLength: Long = file.endRow - file.beginRow

  override def getLocations: Array[String] = Array.empty

  private val originalFieldNames = schema.fields.map(x => x.metadata.getString("original_name"))
  private val basePath: YPath = ypath(new Path(file.path)).toYPath.withColumns(originalFieldNames: _*)

  def ytPath: YPath = {
    if (file.isDynamic) {
      import ru.yandex.spark.yt.serializers.PivotKeysConverter.toRangeLimit
      basePath.withRange(toRangeLimit(file.beginKey, file.keyColumns), toRangeLimit(file.endKey, file.keyColumns))
    } else {
      basePath.withRange(file.beginRow, file.endRow)
    }
  }
}
