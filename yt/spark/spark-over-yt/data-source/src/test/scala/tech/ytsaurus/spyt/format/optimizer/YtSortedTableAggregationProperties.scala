package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{FakeSortShuffleExchangeExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.functions.col
import org.scalacheck.Gen
import tech.ytsaurus.spyt.YtReader
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableAggregationProperties.{AggTest, genAggTest}
import YtSortedTableBaseProperties._

private class YtSortedTableAggregationProperties extends YtSortedTableBaseProperties {
  private def isCorrectInputNode(test: AggTest, project: SparkPlan): Boolean = {
    val columns = test.groupColumns
    project match {
      case ProjectExec(projectList, BatchScanExec(outputList, _, _)) =>
        getColumnNames(projectList) == columns && getColumnNames(outputList) == columns
      case _ =>
        false
    }
  }

  private def isCorrectPlanWithRealExchange(test: AggTest, plan: SparkPlan): Boolean = {
    plan match {
      case HashAggregateExec(_, _, _, groupingExpressions1, _, _, _, _,
      ShuffleExchangeExec(_,
      HashAggregateExec(_, _, _, groupingExpressions2, _, _, _, _, project), _)) =>
        getColumnNames(groupingExpressions1) == test.groupColumns &&
          getColumnNames(groupingExpressions2) == test.groupColumns &&
          isCorrectInputNode(test, project)
      case _ => false
    }
  }

  private def isCorrectPlanWithFakeExchange(test: AggTest, plan: SparkPlan): Boolean = {
    plan match {
      case HashAggregateExec(_, _, _, groupingExpressions1, _, _, _, _,
      HashAggregateExec(_, _, _, groupingExpressions2, _, _, _, _,
      shuffle: FakeSortShuffleExchangeExec)) =>
        getColumnNames(groupingExpressions1) == test.groupColumns &&
          getColumnNames(groupingExpressions2) == test.groupColumns &&
          isCorrectInputNode(test, shuffle.child)
      case _ => false
    }
  }

  private def isCorrectPlan(test: AggTest, plan: SparkPlan): Boolean = {
    isCorrectPlanWithRealExchange(test, plan) || isCorrectPlanWithFakeExchange(test, plan)
  }

  it should "optimize" in {
    withConfs(conf) {
      forAll(genAggTest, minSuccessful(15)) {
        case test@AggTest(source, groupColumns) =>
          beforeEach()
          val sortedData = writeSortedData(source, tmpPath)
          val expected = sortedData
            .groupBy(s => s.take(groupColumns.length)).mapValues(_.length).toSeq
            .map { case (key, v) => key :+ v }
          val query = spark.read.yt(tmpPath).groupBy(groupColumns.map(col): _*).count()
          val res = query.collect()
          isCorrectPlan(test, query.queryExecution.executedPlan) shouldBe true
          res should contain theSameElementsAs expected.map(Row.fromSeq)
          afterEach()
      }
    }
  }
}

object YtSortedTableAggregationProperties {
  case class AggTest(source: Source, groupColumns: Seq[String])

  def genAggTest: Gen[AggTest] = {
    for {
      schema <- genSchema()
      data <- genData(schema)
      keysNumber <- genPrefixColumns(schema)
      groupColsNumber <- genPrefixColumns(schema)
    } yield {
      AggTest(Source(schema, keysNumber, data), groupColsNumber)
    }
  }
}
