package ru.yandex.spark.yt.format.optimizer

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{DependentHashShuffleExchangeExec, FakeHashShuffleExchangeExec, FakeSortShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.{SparkSession, Strategy}
import ru.yandex.spark.yt.format.optimizer.YtSortedTableStrategy.getAttributes

case class YtSortedTableStrategy(spark: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case LogicalSortedMarker(keys, inner) =>
      // aggregation
      val sortOrders = getAttributes(keys, inner.output).map(SortOrder(_, Ascending, NullsFirst, Set.empty))
      if (sortOrders.isEmpty) {
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
        plannedLater :: Nil
      } else {
        logInfo(s"Fake hash shuffle inserted to plan")
        new FakeHashShuffleExchangeExec(attributes, plannedLater) :: Nil
      }
    case LogicalDependentHashMarker(condition, pivots, inner) =>
      // one side join
      val plannedLater = planLater(inner)
      logInfo("Dependent hash shuffle inserted to plan")
      new DependentHashShuffleExchangeExec(condition, pivots, plannedLater, noUserSpecifiedNumPartition = false) :: Nil
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
