package ru.yandex.spark.yt.wrapper

import java.time.{Duration => JavaDuration}
import java.util.Optional
import ru.yandex.bolts.collection.{Option => BoltsOption}

import scala.concurrent.duration.Duration

object YtJavaConverters {
  def toJavaDuration(timeout: Duration): JavaDuration = {
    JavaDuration.ofMillis(timeout.toMillis)
  }

  def toOptional[T](x: Option[T]): Optional[T] = x match {
    case Some(value) => Optional.of(value)
    case None => Optional.empty()
  }

  def toOption[T](opt: BoltsOption[T]): Option[T] = {
    if (opt.isPresent) Some(opt.get()) else None
  }
}
