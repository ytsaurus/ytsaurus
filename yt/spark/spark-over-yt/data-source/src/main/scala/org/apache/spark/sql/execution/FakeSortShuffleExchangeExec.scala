package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}

// child data is divided by some pivots, also data inside partition is sorted
case class FakeSortShuffleExchangeExec(ordering: Seq[SortOrder], child: SparkPlan)
  extends FakeShuffleExchangeExec(child) {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = {
    ordering
  }

  override def nodeName: String = "FakeSortExchange"

  override val outputPartitioning: Partitioning = {
    RangePartitioning(ordering, child.outputPartitioning.numPartitions)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}
