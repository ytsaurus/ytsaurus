package ru.yandex.spark.yt.format

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.StructType

case class YtInputSplit(path: YtPath, start: Long, length: Long, schema: StructType) extends InputSplit {
  override def getLength: Long = length

  override def getLocations: Array[String] = Array.empty

  private val startRow: Long = path.startRow + start

  def getFullPath = s"/${path.stringPath}{${schema.fieldNames.mkString(",")}}[#$startRow:#${startRow + length}]"
}
