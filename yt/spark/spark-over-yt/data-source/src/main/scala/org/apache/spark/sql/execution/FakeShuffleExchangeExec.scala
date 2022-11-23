package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

abstract class FakeShuffleExchangeExec(child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute]

  def outputOrdering: Seq[SortOrder]

  def nodeName: String

  def outputPartitioning: Partitioning

  protected override def doExecute(): RDD[InternalRow] = {
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
