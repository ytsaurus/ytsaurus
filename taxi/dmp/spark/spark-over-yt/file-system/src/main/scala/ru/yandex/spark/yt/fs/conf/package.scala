package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf

import scala.util.Try

package object conf {

  trait ConfProvider {
    def getYtConf(name: String): Option[String]

    def getYtConf[T](conf: ConfigEntry[T]): Option[T] = {
      conf.get(getYtConf(conf.name))
    }

    def ytConf[T](conf: ConfigEntry[T]): T = {
      getYtConf(conf).get
    }
  }

  implicit class SparkYtSqlContext(sqlContext: SQLContext) extends ConfProvider {
    private val configurationPrefix = "spark.yt"

    override def getYtConf(name: String): Option[String] = {
      Try(sqlContext.getConf(s"$configurationPrefix.$name")).toOption
    }
  }

  implicit class SparkYtSqlConf(sqlConf: SQLConf) extends ConfProvider {
    private val configurationPrefix = "spark.hadoop.yt"

    override def getYtConf(name: String): Option[String] = {
      val confName = s"$configurationPrefix.$name"
      if (sqlConf.contains(confName)) {
        Some(sqlConf.getConfString(confName))
      } else None
    }
  }

  implicit class SparkYtSparkConf(sparkConf: SparkConf) extends ConfProvider {
    private val configurationPrefix = "spark.yt"

    override def getYtConf(name: String): Option[String] = {
      sparkConf.getOption(s"$configurationPrefix.$name")
    }
  }

  implicit class SparkYtHadoopConfiguration(configuration: Configuration) extends ConfProvider {
    private val configurationPrefix = "yt"

    override def getYtConf(name: String): Option[String] = {
      Option(configuration.get(s"$configurationPrefix.$name"))
    }

    def setYtConf(name: String, value: Any): Unit = {
      configuration.set(s"$configurationPrefix.$name", value.toString)
    }

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): Unit = {
      setYtConf(configEntry.name, configEntry.set(value))
    }
  }

  implicit class OptionsConf(options: Map[String, String]) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = options.get(name)
  }

}
