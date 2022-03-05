package ru.yandex.spark.yt.wrapper

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

import java.time.{Duration => JavaDuration}
import java.util.Optional
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

object YtJavaConverters {
  def toJavaDuration(timeout: Duration): JavaDuration = {
    JavaDuration.ofMillis(timeout.toMillis)
  }

  def toScalaDuration(timeout: JavaDuration): Duration = {
    Duration.apply(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

  def toOptional[T](x: Option[T]): Optional[T] = x match {
    case Some(value) => Optional.of(value)
    case None => Optional.empty()
  }

  def toOption[T](opt: Optional[T]): Option[T] = {
    if (opt.isPresent) Some(opt.get()) else None
  }

  implicit class RichJavaMap[T](jmap: java.util.Map[String, T]) {
    def getOrThrow(key: String): T = {
      getOption(key).getOrElse(throw new IllegalArgumentException(s"Key not found: $key"))
    }

    def getOption(key: String): Option[T] = {
      if (!jmap.containsKey(key)) None
      else Some(jmap.get(key))
    }
  }

  implicit class RichYTreeMap(jmap: java.util.Map[String, YTreeNode]) {
    def getOptionNode[T](key: String): Option[YTreeNode] = {
      val res = jmap.getOrThrow(key)
      if (res.isEntityNode) {
        None
      } else {
        Some(res)
      }
    }

    def getOptionString(key: String): Option[String] = {
      getOptionNode(key).map(_.stringValue())
    }
  }
}
