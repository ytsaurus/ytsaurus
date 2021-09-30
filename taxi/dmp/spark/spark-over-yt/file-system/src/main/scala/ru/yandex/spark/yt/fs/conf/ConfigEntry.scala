package ru.yandex.spark.yt.fs.conf

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import ru.yandex.spark.yt.wrapper.Utils.parseDuration

import scala.concurrent.duration._

abstract class ConfigEntry[T](val name: String,
                              val default: Option[T] = None) {
  def get(value: String): T

  def get(value: Option[String]): Option[T] = value.map(get).orElse(default)

  def set(value: T): String = value.toString

  def fromJson(value: String)(implicit decoder: Decoder[T]): T = fromJsonTyped[T](value)

  def fromJsonTyped[S](value: String)(implicit decoder: Decoder[S]): S = {
    decode[S](value) match {
      case Right(res) => res
      case Left(error) => throw error
    }
  }

  def toJson(value: T)(implicit encoder: Encoder[T]): String = toJsonTyped[T](value)

  def toJsonTyped[S](value: S)(implicit encoder: Encoder[S]): String = {
    value.asJson.noSpaces
  }
}

class IntConfigEntry(name: String, default: Option[Int] = None) extends ConfigEntry[Int](name, default) {
  override def get(value: String): Int = value.toInt
}

class LongConfigEntry(name: String, default: Option[Long] = None) extends ConfigEntry[Long](name, default) {
  override def get(value: String): Long = value.toLong
}

class BooleanConfigEntry(name: String, default: Option[Boolean] = None) extends ConfigEntry[Boolean](name, default) {
  override def get(value: String): Boolean = value.toBoolean
}

class DurationSecondsConfigEntry(name: String, default: Option[Duration] = None) extends ConfigEntry[Duration](name, default) {
  override def get(value: String): Duration = parseDuration(value)
}

class StringConfigEntry(name: String, default: Option[String] = None) extends ConfigEntry[String](name, default) {
  override def get(value: String): String = value
}

class StringListConfigEntry(name: String, default: Option[Seq[String]] = None) extends ConfigEntry[Seq[String]](name, default) {
  override def get(value: String): Seq[String] = fromJson(value)

  override def set(value: Seq[String]): String = toJson(value)
}

class StringMapConfigEntry(name: String, default: Option[Map[String, String]] = None)
  extends ConfigEntry[Map[String, String]](name, default) {
  override def get(value: String): Map[String, String] = fromJson(value)

  override def set(value: Map[String, String]): String = toJson(value)
}
