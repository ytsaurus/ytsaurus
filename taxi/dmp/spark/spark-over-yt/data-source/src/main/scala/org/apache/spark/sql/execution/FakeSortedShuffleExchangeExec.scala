package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

class FakeSortedShuffleExchangeExec(fakeOutputPartitioning: Partitioning,
                                    child: SparkPlan,
                                    noUserSpecifiedNumPartition: Boolean = true)
  extends ShuffleExchangeExec(fakeOutputPartitioning, child, noUserSpecifiedNumPartition) {

  override def output: Seq[Attribute] = child.output

  private def getRangePartitioning: RangePartitioning = fakeOutputPartitioning match {
    case r: RangePartitioning => r
    case _ => throw new IllegalArgumentException("This node supports only range partitioning")
  }

  override def outputOrdering: Seq[SortOrder] = getRangePartitioning.ordering

  override def nodeName: String = "FakeExchange"

  override val outputPartitioning: Partitioning = {
    RangePartitioning(getRangePartitioning.ordering, child.outputPartitioning.numPartitions)
  }

  def this(sortOrder: Seq[SortOrder],
           child: SparkPlan,
           noUserSpecifiedNumPartition: Boolean) {
    // We don't know num partitions when child is PlanLater, so here we get 0 anyway
    this(RangePartitioning(sortOrder, child.outputPartitioning.numPartitions), child, noUserSpecifiedNumPartition)
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    inputRDD
  }
}
