package org.apache.spark.yt.test

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigEntry, FallbackConfigEntry}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object Utils {
  type SparkConfigEntry[T] = ConfigEntry[T]

  def defaultParallelism(spark: SparkSession): Int = spark.sparkContext.defaultParallelism

  @tailrec
  def defaultConfValue[T](entry: SparkConfigEntry[T], localDefaultConf: SparkConf): Option[String] = {
    if (localDefaultConf.contains(entry)) {
      Some(localDefaultConf.get(entry).toString)
    } else {
      entry.defaultValue match {
        case Some(_) => Some(entry.defaultValueString)
        case None =>
          entry match {
            case e: FallbackConfigEntry[T] => defaultConfValue(e.fallback, localDefaultConf)
            case _ => None
          }
      }
    }
  }
}
