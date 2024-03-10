package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.fs.conf.ConfigEntry

import scala.concurrent.duration._
import scala.language.postfixOps

object SparkYtConfiguration {
  import ConfigEntry.implicits._

  object Write {
    private val prefix = "write"

    case object BatchSize extends ConfigEntry[Int](s"$prefix.batchSize", Some(500000))

    case object MiniBatchSize extends ConfigEntry[Int](s"$prefix.miniBatchSize", Some(1000))

    case object DynBatchSize extends ConfigEntry[Int](s"$prefix.dynBatchSize", Some(50000))

    case object Timeout extends ConfigEntry[Duration](s"$prefix.timeout", Some(300 seconds))

    case object TypeV3 extends ConfigEntry[Boolean](s"$prefix.typeV3.enabled", Some(false), List(
      s"$prefix.writingTypeV3.enabled", s"$prefix.typeV3Format.enabled"
    ))
  }

  object Read {
    private val prefix = "read"

    case object VectorizedCapacity extends ConfigEntry[Int](s"$prefix.vectorized.capacity", Some(1000))

    case object ArrowEnabled extends ConfigEntry[Boolean](s"$prefix.arrow.enabled", Some(true))

    case object YtPartitioningEnabled extends ConfigEntry[Boolean](s"$prefix.ytPartitioning.enabled", Some(false))

    case object CountOptimizationEnabled
      extends ConfigEntry[Boolean](s"$prefix.countOptimization.enabled", Some(true))

    case object PlanOptimizationEnabled extends ConfigEntry[Boolean](s"$prefix.planOptimization.enabled", Some(false))

    case object TypeV3 extends ConfigEntry[Boolean](s"$prefix.typeV3.enabled", Some(false), List(
      s"$prefix.parsingTypeV3.enabled"
    ))

    object KeyPartitioning {
      private val prefix: String = s"${Read.prefix}.keyPartitioningSortedTables"

      case object Enabled extends ConfigEntry[Boolean](s"$prefix.enabled", Some(false))

      case object UnionLimit extends ConfigEntry[Int](s"$prefix.unionLimit", Some(1))
    }


    object KeyColumnsFilterPushdown {
      private val prefix: String = s"${Read.prefix}.keyColumnsFilterPushdown"

      case object Enabled extends ConfigEntry[Boolean](s"$prefix.enabled", Some(false))

      case object UnionEnabled extends ConfigEntry[Boolean](s"$prefix.union.enabled", Some(false))

      case object YtPathCountLimit extends ConfigEntry[Int](s"$prefix.ytPathCount.limit", Some(100))
    }
  }

  object Transaction {
    private val prefix = "transaction"

    case object Timeout extends ConfigEntry[Duration](s"$prefix.timeout", Some(5 minutes))

    case object PingInterval extends ConfigEntry[Duration](s"$prefix.pingInterval", Some(30 seconds))

  }

  object GlobalTransaction {
    private val prefix = "globalTransaction"

    case object Timeout extends ConfigEntry[Duration](s"$prefix.timeout", Some(2 minutes))

    case object Enabled extends ConfigEntry[Boolean](s"$prefix.enabled", Some(false))

    case object Id extends ConfigEntry[String](s"$prefix.id", None)
  }

  object Schema {
    private val prefix = "schema"

    case object ForcingNullableIfNoMetadata extends ConfigEntry[Boolean](s"$prefix.forcingNullableIfNoMetadata.enabled", Some(true))
  }

}
