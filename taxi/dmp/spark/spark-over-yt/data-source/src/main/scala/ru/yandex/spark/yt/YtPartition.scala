package ru.yandex.spark.yt

import org.apache.spark.Partition

case class YtPartition(index: Int, startRowInclusive: Long, endRowExclusive: Long) extends Partition
