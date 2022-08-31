package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

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
}
