package ru.yandex.spark.yt.file

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.StructType

case class YtInputSplit(path: String, start: Long, length: Long, schema: StructType) extends InputSplit {
  override def getLength: Long = length

  override def getLocations: Array[String] = Array.empty

  def getFullPath = s"/$path{${schema.fieldNames.mkString(",")}}[#${start}:#${start + length}]"
}
