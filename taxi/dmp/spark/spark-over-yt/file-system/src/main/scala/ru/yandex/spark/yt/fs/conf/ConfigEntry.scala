package ru.yandex.spark.yt.fs.conf

import org.apache.spark.sql.types.StructType

abstract class ConfigEntry[T](val name: String,
                              val default: Option[T] = None) {
  def get(value: String): T

  def get(value: Option[String]): Option[T] = value.map(get).orElse(default)

  def set(value: T): String = value.toString
}

class IntConfigEntry(name: String, default: Option[Int] = None) extends ConfigEntry[Int](name, default) {
  override def get(value: String): Int = value.toInt
}

class StringConfigEntry(name: String, default: Option[String] = None) extends ConfigEntry[String](name, default) {
  override def get(value: String): String = value
}

class StringListConfigEntry(name: String, default: Option[Seq[String]] = None) extends ConfigEntry[Seq[String]](name, default) {
  override def get(value: String): Seq[String] = value.split(",").map(_.trim)

  override def set(value: Seq[String]): String = value.mkString(",")
}

class StructTypeConfigEntry(name: String) extends ConfigEntry[StructType](name, None) {
  override def get(value: String): StructType = ConfigTypeConverter.sparkType(value).asInstanceOf[StructType]

  override def set(value: StructType): String = ConfigTypeConverter.stringType(value)
}
