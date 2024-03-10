package tech.ytsaurus.spyt

import org.apache.spark.SparkConf

object Utils {

  def isIpV6Host(host: String): Boolean = {
    host != null && host.contains(":")
  }

  def removeBracketsIfIpV6Host(host: String): String = {
    if (isIpV6Host(host) && host.startsWith("[")) {
      host.substring(1, host.length - 1)
    } else {
      host
    }
  }

  def addBracketsIfIpV6Host(host: String): String = {
    if (isIpV6Host(host) && !host.startsWith("[")) {
      s"[$host]"
    } else {
      host
    }
  }

  def filterSparkConf(conf: SparkConf): Map[String, String] = {
    conf.getAll.collect {
      case (key, value) if key.startsWith("spark.yt") || key.startsWith("spark.hadoop.yt") =>
        key.toUpperCase().replace(".", "_") -> value
    }.toMap
  }
}
