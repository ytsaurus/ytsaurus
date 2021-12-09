package ru.yandex.spark.yt.format.conf

import ru.yandex.spark.yt.fs.conf.{BooleanConfigEntry, DurationSecondsConfigEntry, IntConfigEntry, StringConfigEntry}

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

    case object ArrowEnabled extends BooleanConfigEntry(s"$prefix.arrow.enabled", Some(true))

    case object ParsingTypeV3 extends BooleanConfigEntry(s"$prefix.parsingTypeV3.enabled", Some(false))

    object KeyColumnsFilterPushdown {
      private val prefix: String = s"${Read.prefix}.keyColumnsFilterPushdown"

      case object Enabled extends BooleanConfigEntry(s"$prefix.enabled", Some(false))

      case object UnionEnabled extends BooleanConfigEntry(s"$prefix.union.enabled", Some(false))

      case object YtPathCountLimit extends IntConfigEntry(s"$prefix.ytPathCount.limit", Some(100))
    }
  }

  object Transaction {
    private val prefix = "transaction"

    case object Timeout extends DurationSecondsConfigEntry(s"$prefix.timeout", Some(5 minutes))

    case object PingInterval extends DurationSecondsConfigEntry(s"$prefix.pingInterval", Some(30 seconds))

  }

  object GlobalTransaction {
    private val prefix = "globalTransaction"

    case object Timeout extends DurationSecondsConfigEntry(s"$prefix.timeout", Some(2 minutes))

    case object Enabled extends BooleanConfigEntry(s"$prefix.enabled", Some(false))

    case object Id extends StringConfigEntry(s"$prefix.id", None)
  }

}
