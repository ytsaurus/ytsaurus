package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SQLContext, SparkSession}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode

import scala.util.Try

package object conf {
  val YT_MIN_PARTITION_BYTES = "spark.yt.minPartitionBytes"

  trait ConfProvider {
    def getYtConf(name: String): Option[String]

    def getAllKeys: Seq[String]

    def getYtConf[T](conf: ConfigEntry[T]): Option[T] = {
      conf.get(getYtConf(conf.name))
    }

    def ytConf[T](conf: ConfigEntry[T]): T = {
      getYtConf(conf).get
    }

    def ytConf[T](conf: MultiConfigEntry[T]): Map[String, T] = {
      conf.get(getAllKeys, this)
    }
  }

  private def dropPrefix(seq: Seq[String], prefix: String): Seq[String] = {
    seq.collect {
      case str if str.startsWith(prefix) => str.drop(prefix.length + 1)
    }
  }

  implicit class SparkYtSqlContext(sqlContext: SQLContext) extends ConfProvider {
    private val configurationPrefix = "spark.yt"

    override def getYtConf(name: String): Option[String] = {
      Try(sqlContext.getConf(s"$configurationPrefix.$name")).toOption
    }

    override def getAllKeys: Seq[String] = {
      dropPrefix(sqlContext.sparkSession.conf.getAll.keys.toList, configurationPrefix)
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

    override def getAllKeys: Seq[String] = {
      dropPrefix(sqlConf.getAllDefinedConfs.map(_._1), configurationPrefix)
    }
  }

  implicit class SparkYtSparkConf(sparkConf: SparkConf) extends ConfProvider {
    private val configurationPrefix = "spark.yt"

    override def getYtConf(name: String): Option[String] = {
      sparkConf.getOption(s"$configurationPrefix.$name")
    }

    def setYtConf(name: String, value: Any): SparkConf = {
      sparkConf.set(s"$configurationPrefix.$name", value.toString)
    }

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): SparkConf = {
      setYtConf(configEntry.name, configEntry.set(value))
    }

    override def getAllKeys: Seq[String] = {
      dropPrefix(sparkConf.getAll.map(_._1), configurationPrefix)
    }
  }

  implicit class SparkYtSparkSession(spark: SparkSession) extends ConfProvider {
    private val configurationPrefix = "spark.yt"

    override def getYtConf(name: String): Option[String] = {
      spark.conf.getOption(s"$configurationPrefix.$name")
    }

    def setYtConf(name: String, value: Any): Unit = {
      spark.conf.set(s"$configurationPrefix.$name", value.toString)
    }

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): Unit = {
      setYtConf(configEntry.name, configEntry.set(value))
    }

    override def getAllKeys: Seq[String] = {
      dropPrefix(spark.conf.getAll.keys.toList, configurationPrefix)
    }
  }

  implicit class SparkYtHadoopConfiguration(configuration: Configuration) extends ConfProvider {
    private val configurationPrefix = "yt"

    override def getYtConf(name: String): Option[String] = {
      Option(configuration.get(s"$configurationPrefix.$name"))
    }

    def getYtSpecConf(name: String): Map[String, YTreeNode] = {
      import scala.collection.JavaConverters._
      configuration.asScala.collect {
        case entry if entry.getKey.startsWith(s"spark.yt.$name") =>
          val key = entry.getKey.drop(s"spark.yt.$name.".length)
          val value = YTreeTextSerializer.deserialize(entry.getValue)
          key -> value
      }.toMap
    }

    def setYtConf(name: String, value: Any): Unit = {
      configuration.set(s"$configurationPrefix.$name", value.toString)
    }

    def setYtConf[T](configEntry: ConfigEntry[T], value: T): Unit = {
      setYtConf(configEntry.name, configEntry.set(value))
    }

    override def getAllKeys: Seq[String] = {
      import scala.collection.JavaConverters._
      dropPrefix(configuration.asScala.map(_.getKey).toList, configurationPrefix)
    }
  }

  implicit class OptionsConf(options: Map[String, String]) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = options.get(name)

    override def getAllKeys: Seq[String] = options.keys.toList
  }

  implicit class CaseInsensitiveMapConf(options: CaseInsensitiveStringMap) extends ConfProvider {
    override def getYtConf(name: String): Option[String] = if (options.containsKey(name)) {
      Some(options.get(name))
    } else None

    override def getAllKeys: Seq[String] = {
      import scala.collection.JavaConverters._
      options.keySet().asScala.toList
    }
  }

}
