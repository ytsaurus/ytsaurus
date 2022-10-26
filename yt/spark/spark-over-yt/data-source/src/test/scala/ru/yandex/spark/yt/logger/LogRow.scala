package ru.yandex.spark.yt.logger

import org.apache.log4j.Level

case class LogRow(name: String, msg: String, level: Level,
                  sparkComponent: SparkComponent,
                  partitionId: Option[Int] = None,
                  info: Map[String, String] = Map.empty)
