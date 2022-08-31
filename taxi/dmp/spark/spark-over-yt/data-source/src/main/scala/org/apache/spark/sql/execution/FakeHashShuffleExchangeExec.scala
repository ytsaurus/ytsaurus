package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashClusteredDistribution, HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

// Frankenstein: child data is divided by hash, data inside partition is sorted
case class FakeHashShuffleExchangeExec(attrs: Seq[AttributeReference], child: SparkPlan)
  extends FakeShuffleExchangeExec(child) {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = {
    attrs.map(SortOrder(_, Ascending, NullsFirst, Set.empty))
  }

  override def nodeName: String = "FakeHashExchange"

  override val outputPartitioning: Partitioning = {
    HashPartitioning(attrs, child.outputPartitioning.numPartitions)
  }
}
