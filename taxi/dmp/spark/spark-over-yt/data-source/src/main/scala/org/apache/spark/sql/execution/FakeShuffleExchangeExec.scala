package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

abstract class FakeShuffleExchangeExec(child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute]

  def outputOrdering: Seq[SortOrder]

  def nodeName: String

  def outputPartitioning: Partitioning

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${consume(ctx, input)}
     """.stripMargin
  }
}
