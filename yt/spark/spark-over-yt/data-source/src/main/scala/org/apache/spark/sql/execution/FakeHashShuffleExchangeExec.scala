package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}

// Frankenstein: child data is divided by hash, data inside partition is sorted
case class FakeHashShuffleExchangeExec(attrs: Seq[AttributeReference], child: SparkPlan)
  extends FakeShuffleExchangeExec(child) {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = {
    attrs.map(SortOrder(_, Ascending, NullsFirst, Seq.empty))
  }

  override def nodeName: String = "FakeHashExchange"

  override val outputPartitioning: Partitioning = {
    HashPartitioning(attrs, child.outputPartitioning.numPartitions)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}
