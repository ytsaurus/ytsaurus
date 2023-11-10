package tech.ytsaurus.spyt.fs.conf

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import org.apache.log4j.Level
import tech.ytsaurus.spyt.wrapper.Utils.parseDuration

import scala.concurrent.duration._

/**
 * Shorthand class for working with spark properties.
 * @param name primary entry name which is used for get and set
 * @param default default value
 * @param aliases a list of entry aliases if the property can't be retrieved by its primary name ordered by priority.
 */
class ConfigEntry[T : ValueAdapter](val name: String,
                                    val default: Option[T] = None,
                                    val aliases: List[String] = Nil) {
  import ConfigEntry.{fromJsonTyped, toJsonTyped}

  private val valueAdapter = implicitly[ValueAdapter[T]]

  def get(value: String): T = valueAdapter.get(value)

  def get(value: Option[String]): Option[T] = value.map(get).orElse(default)

  def set(value: T): String = valueAdapter.set(value)

  def fromJson(value: String)(implicit decoder: Decoder[T]): T = fromJsonTyped[T](value)

  def toJson(value: T)(implicit encoder: Encoder[T]): String = toJsonTyped[T](value)
}

trait ValueAdapter[T] {
  def get(value: String): T
  def set(value: T): String = value.toString
}

object ConfigEntry {
  object implicits {
    implicit val intAdapter: ValueAdapter[Int] = (value: String) => value.toInt
    implicit val longAdapter: ValueAdapter[Long] = (value: String) => value.toLong
    implicit val boolAdapter: ValueAdapter[Boolean] = (value: String) => value.toBoolean
    implicit val durationAdapter: ValueAdapter[Duration] = (value: String) => parseDuration(value)
    implicit val stringAdapter: ValueAdapter[String] = (value: String) => value
    implicit val stringListAdapter: ValueAdapter[Seq[String]] = new ValueAdapter[Seq[String]] {
      override def get(value: String): Seq[String] = fromJsonTyped[Seq[String]](value)
      override def set(value: Seq[String]): String = toJsonTyped(value)
    }

    implicit val stringMapAdapter: ValueAdapter[Map[String, String]] = new ValueAdapter[Map[String, String]] {
      override def get(value: String): Map[String, String] = fromJsonTyped[Map[String, String]](value)
      override def set(value: Map[String, String]): String = toJsonTyped(value)
    }

    implicit val levelAdapter: ValueAdapter[Level] = (value: String) => Level.toLevel(value)
  }

  def fromJsonTyped[S](value: String)(implicit decoder: Decoder[S]): S = {
    decode[S](value) match {
      case Right(res) => res
      case Left(error) => throw error
    }
  }

  def toJsonTyped[S](value: S)(implicit encoder: Encoder[S]): String = {
    value.asJson.noSpaces
  }
}
