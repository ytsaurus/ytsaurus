package ru.yandex.spark.yt.format.conf

import ru.yandex.spark.yt.fs.conf.{DurationSecondsConfigEntry, IntConfigEntry}

import scala.concurrent.duration._
import scala.language.postfixOps

object SparkYtConfiguration {

  object Write {
    private val prefix = "write"

    case object BatchSize extends IntConfigEntry(s"$prefix.batchSize", Some(500000))

    case object MiniBatchSize extends IntConfigEntry(s"$prefix.miniBatchSize", Some(1000))

    case object Timeout extends DurationSecondsConfigEntry(s"$prefix.timeout", Some(60 seconds))

  }

  object Read {
    private val prefix = "read"

    case object VectorizedCapacity extends IntConfigEntry(s"$prefix.vectorized.capacity", Some(1000))

  }

  object Transaction {
    private val prefix = "transaction"

    case object Timeout extends DurationSecondsConfigEntry(s"$prefix.timeout", Some(5 minutes))

    case object PingInterval extends DurationSecondsConfigEntry(s"$prefix.pingInterval", Some(30 seconds))

  }

}
