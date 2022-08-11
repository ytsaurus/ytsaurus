package ru.yandex.spark.yt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.v2.{YtScan, YtTable}

class YtSortedTableMarkerRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a@Aggregate(gE, aE, relation@DataSourceV2ScanRelation(_, scan: YtScan, _)) =>
      val attrs =
        gE.map {
          case a: AttributeReference => Some(a)
          case _ => None
        }
      if (attrs.forall(_.isDefined)) {
        val vars = attrs.map(_.get.name)
        val newScanO = scan.tryKeyPartitioning(Some(vars))
        newScanO match {
          case Some(newScan) => Aggregate(gE, aE, LogicalSortedMarker(vars, relation.copy(scan = newScan)))
          case None => a
        }
      } else {
        a
      }
  }
}
