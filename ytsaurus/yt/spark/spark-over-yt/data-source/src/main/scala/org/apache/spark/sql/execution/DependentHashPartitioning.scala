package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution, Partitioning}
import org.apache.spark.sql.types.{DataType, IntegerType}
import tech.ytsaurus.spyt.common.utils.TuplePoint

// copied from hash partitioning
// contains partitioning that conformed with another join side
case class DependentHashPartitioning(expressions: Seq[Expression], pivots: Seq[TuplePoint])
  extends Expression with Partitioning with Unevaluable {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: HashClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case _ => false
      }
    }
  }

  override val numPartitions: Int = pivots.length + 1

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(expressions = newChildren)
}
