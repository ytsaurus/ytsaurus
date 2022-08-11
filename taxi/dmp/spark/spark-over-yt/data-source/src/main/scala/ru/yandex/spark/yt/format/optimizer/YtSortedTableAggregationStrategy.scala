package ru.yandex.spark.yt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.{Ascending, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.{FakeSortedShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.v2.YtScan
import org.apache.spark.sql.{SparkSession, Strategy}
import ru.yandex.spark.yt.serializers.SchemaConverter

case class YtSortedTableAggregationStrategy(spark: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LogicalSortedMarker(keys, relation@DataSourceV2ScanRelation(_, scan: YtScan, output)) =>
      val plannedLater = planLater(relation)
      // always true ru/yandex/spark/yt/format/optimizer/YtSortedTableMarkerRule.scala:25
      if (scan.supportsKeyPartitioning) {
        val attributes = keys.map(s => output.find(_.name == s)).takeWhile(_.isDefined).map(_.get)
        if (attributes.isEmpty) {
          plannedLater :: Nil
        } else {
          val sortOrder = attributes.map(SortOrder(_, Ascending, NullsFirst, Set.empty))
          new FakeSortedShuffleExchangeExec(sortOrder, plannedLater, noUserSpecifiedNumPartition = false) :: Nil
        }
      } else {
        plannedLater :: Nil
      }
    case LogicalSortedMarker(_, notRelation) =>
      // impossible case
      logError("Logical sorted marker contains not relation, marker will be deleted")
      planLater(notRelation) :: Nil
    case _ =>
      Nil
  }
}
