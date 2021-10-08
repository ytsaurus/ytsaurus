package ru.yandex.spark.yt.format

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.yson.UInt64Type
import org.apache.spark.sql.{SparkSession, Strategy}

class YtUInt64StrategyChecker(spark: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Aggregate(_, aggregateExpressions, _) =>
      aggregateExpressions.foreach {
        case AttributeReference(name, dataType, _, _) if dataType == UInt64Type =>
          throw new IllegalStateException(s"Cannot aggregate column '$name' with uint64 type")
        case _ =>
      }
      Nil
    case _ =>
      Nil
  }
}
