package ru.yandex.spark.yt

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

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

}
