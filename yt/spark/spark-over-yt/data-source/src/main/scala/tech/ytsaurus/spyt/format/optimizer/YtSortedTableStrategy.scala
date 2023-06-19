package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{DependentHashShuffleExchangeExec, FakeHashShuffleExchangeExec, FakeSortShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.{SparkSession, Strategy}
import YtSortedTableStrategy.getAttributes

case class YtSortedTableStrategy(spark: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LogicalSortedMarker(keys, inner) =>
      // aggregation
      val sortOrders = getAttributes(keys, inner.output).map(SortOrder(_, Ascending, NullsFirst, Seq.empty))
      if (sortOrders.isEmpty) {
        logWarning(s"Sort marker dropped. Attributes are inconsistency")
        planLater(inner) :: Nil
      } else {
        logInfo(s"Fake sort shuffle inserted to plan")
        new FakeSortShuffleExchangeExec(sortOrders, planLater(inner)) :: Nil
      }
    case LogicalHashedMarker(keys, inner) =>
      // join
      val plannedLater = planLater(inner)
      val attributes = getAttributes(keys, inner.output)
      if (attributes.isEmpty) {
        logWarning(s"Shuffle marker dropped. Attributes are inconsistency")
        plannedLater :: Nil
      } else {
        logInfo(s"Fake hash shuffle inserted to plan")
        new FakeHashShuffleExchangeExec(attributes, plannedLater) :: Nil
      }
    case LogicalDependentHashMarker(condition, pivots, inner) =>
      // one side join
      val plannedLater = planLater(inner)
      logInfo("Dependent hash shuffle inserted to plan")
      new DependentHashShuffleExchangeExec(condition, pivots, plannedLater) :: Nil
    case _ =>
      Nil
  }
}

object YtSortedTableStrategy {
  private def getAttributes(keys: Seq[String], output: Seq[Attribute]): Seq[AttributeReference] = {
    keys
      .map(s => output.collectFirst { case a: AttributeReference if a.name == s => a })
      .takeWhile(_.isDefined).map(_.get)
  }
}
