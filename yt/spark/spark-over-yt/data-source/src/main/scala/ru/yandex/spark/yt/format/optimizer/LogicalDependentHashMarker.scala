package ru.yandex.spark.yt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import ru.yandex.spark.yt.common.utils.TuplePoint

case class LogicalDependentHashMarker(condition: Seq[Expression], pivots: Seq[TuplePoint],
                                      child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalDependentHashMarker = copy(child = newChild)
}
