package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import scala.util.Try

object SparkYtOptions {
  private val configurationPrefix = "spark.yt"

  def deserialize(configuration: Configuration): Map[String, String] = {
    val optionsKeys = configuration.get(s"$configurationPrefix.options").trim.split(",")
    optionsKeys.map { key => key -> configuration.get(s"$configurationPrefix.$key") }.toMap
  }

  def serialize(options: Map[String, String], configuration: Configuration): Unit = {
    options.foreach { case (key, value) =>
      configuration.set(s"$configurationPrefix.$key", value)
    }
    configuration.set(s"$configurationPrefix.options", options.keys.mkString(","))
  }


  implicit class YtOptionsSqlContext(sqlContext: SQLContext) {
    def getYtConf(name: String): Option[String] = {
      Try(sqlContext.getConf(s"$configurationPrefix.$name")).toOption
    }

    def setYtConf(name: String, value: Any): Unit = {
      sqlContext.setConf(s"$configurationPrefix.$name", value.toString)
    }
  }

  implicit class YtOptionsSparkConf(sparkConf: SparkConf) {
    def getYtConf(name: String): Option[String] = {
      sparkConf.getOption(s"$configurationPrefix.$name")
    }
  }

  implicit class YtOptionsConfiguration(configuration: Configuration) {
    def ytConf(name: String): String = {
      configuration.get(s"$configurationPrefix.$name")
    }

    def getYtConf(name: String): Option[String] = {
      Option(configuration.get(s"$configurationPrefix.$name"))
    }

    def setYtConf(name: String, value: Any): Unit = {
      configuration.set(s"$configurationPrefix.$name", value.toString)
    }
  }

}
