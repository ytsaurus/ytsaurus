package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class LogicalHashedMarker(keys: Seq[String], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def maxRows: Option[Long] = child.maxRows

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalHashedMarker = copy(child = newChild)
}
