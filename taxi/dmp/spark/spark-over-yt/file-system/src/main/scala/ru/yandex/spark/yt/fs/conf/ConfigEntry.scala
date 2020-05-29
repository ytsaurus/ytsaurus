package ru.yandex.spark.yt.fs.conf

import org.apache.spark.sql.types.StructType

import scala.concurrent.duration._

abstract class ConfigEntry[T](val name: String,
                              val default: Option[T] = None) {
  def get(value: String): T

  def get(value: Option[String]): Option[T] = value.map(get).orElse(default)

  def set(value: T): String = value.toString
}

class IntConfigEntry(name: String, default: Option[Int] = None) extends ConfigEntry[Int](name, default) {
  override def get(value: String): Int = value.toInt
}

class DurationSecondsConfigEntry(name: String, default: Option[Duration] = None) extends ConfigEntry[Duration](name, default) {
  override def get(value: String): Duration = value.toInt.seconds
}

class StringConfigEntry(name: String, default: Option[String] = None) extends ConfigEntry[String](name, default) {
  override def get(value: String): String = value
}

class BooleanConfigEntry(name: String, default: Option[Boolean] = None) extends ConfigEntry[Boolean](name, default) {
  override def get(value: String): Boolean = value.toBoolean
}

class StringListConfigEntry(name: String, default: Option[Seq[String]] = None) extends ConfigEntry[Seq[String]](name, default) {
  override def get(value: String): Seq[String] = value.split(",").map(_.trim).filter(_.nonEmpty)

  override def set(value: Seq[String]): String = value.mkString(",")
}

class StructTypeConfigEntry(name: String) extends ConfigEntry[StructType](name, None) {
  override def get(value: String): StructType = ConfigTypeConverter.sparkType(value).asInstanceOf[StructType]

  override def set(value: StructType): String = ConfigTypeConverter.stringType(value)
}
