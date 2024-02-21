package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution._
import org.scalacheck.Gen
import tech.ytsaurus.spyt.YtReader
import YtSortedTableBaseProperties._
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableJoinProperties.{JoinTest, genJoinTest}
import tech.ytsaurus.spyt.wrapper.YtWrapper

private class YtSortedTableJoinProperties extends YtSortedTableBaseProperties {
  private def isCorrectInputNode(source: Source, project: SparkPlan): Boolean = {
    val columns = source.schema.getColumnNames()
    project match {
      case ProjectExec(projectList, FilterExec(_, BatchScanExec(outputList, _, _))) =>
        getColumnNames(projectList) == columns && getColumnNames(outputList) == columns
      case _ =>
        false
    }
  }

  private def isCorrectPlanWithProcessedBothSideJoin(test: JoinTest, plan: SparkPlan): Boolean = {
    plan match {
      case ProjectExec(_, SortMergeJoinExec(keys1, keys2, _, _,
      f1: FakeHashShuffleExchangeExec, f2: FakeHashShuffleExchangeExec,
      _)) =>
        getColumnNames(keys1) == test.joinColumns && getColumnNames(keys2) == test.joinColumns &&
          isCorrectInputNode(test.source1, f1.child) && isCorrectInputNode(test.source2, f2.child)
      case _ =>
        false
    }
  }

  private def isCorrectPlanWithProcessedOneSideJoin(test: JoinTest, plan: SparkPlan): Boolean = {
    def isCorrectJoinRawChildren(left: SparkPlan, right: SparkPlan): Boolean = {
      (left, right) match {
        case (f1: FakeHashShuffleExchangeExec, SortExec(_, _, f2: DependentHashShuffleExchangeExec, _)) =>
          isCorrectInputNode(test.source1, f1.child) && isCorrectInputNode(test.source2, f2.child)
        case (SortExec(_, _, f1: DependentHashShuffleExchangeExec, _), f2: FakeHashShuffleExchangeExec) =>
          isCorrectInputNode(test.source1, f1.child) && isCorrectInputNode(test.source2, f2.child)
        case _ =>
          false
      }
    }
    plan match {
      case ProjectExec(_, SortMergeJoinExec(keys1, keys2, _, _, left, right, _)) =>
        getColumnNames(keys1) == test.joinColumns && getColumnNames(keys2) == test.joinColumns &&
          isCorrectJoinRawChildren(left, right)
      case _ =>
        false
    }
  }

  private def isCorrectPlanWithNotProcessedJoin(test: JoinTest, plan: SparkPlan): Boolean = {
    plan match {
      case ProjectExec(_, SortMergeJoinExec(keys1, keys2, _, _,
      SortExec(_, _, ShuffleExchangeExec(HashPartitioning(hash1, _), project1, _), _),
      SortExec(_, _, ShuffleExchangeExec(HashPartitioning(hash2, _), project2, _), _),
      _)) =>
        getColumnNames(keys1) == test.joinColumns && getColumnNames(keys2) == test.joinColumns &&
          getColumnNames(hash1) == test.joinColumns && getColumnNames(hash2) == test.joinColumns &&
          isCorrectInputNode(test.source1, project1) && isCorrectInputNode(test.source2, project2)
      case _ =>
        false
    }
  }

  private def isCorrectPlan(test: JoinTest, plan: SparkPlan): Boolean = {
    isCorrectPlanWithNotProcessedJoin(test, plan) ||
      isCorrectPlanWithProcessedOneSideJoin(test, plan) ||
      isCorrectPlanWithProcessedBothSideJoin(test, plan)
  }

  it should "optimize" in {
    withConfs(conf) {
      forAll(genJoinTest, minSuccessful(10)) {
        case test@JoinTest(source1, source2, joinColumns) =>
          beforeEach()
          YtWrapper.createDir(tmpPath)
          val table1 = s"$tmpPath/1"
          val table2 = s"$tmpPath/2"
          val sortedData1 = writeSortedData(source1, table1)
          val sortedData2 = writeSortedData(source2, table2)
          val expected = YtSortedTableJoinProperties.simulateJoin(sortedData1, sortedData2, joinColumns.length)
          val query = spark.read.yt(table1).join(spark.read.yt(table2), joinColumns)
          val res = query.collect()
//          isCorrectPlan(test, query.queryExecution.executedPlan) shouldBe true
          res.length shouldBe expected.length
          res should contain theSameElementsAs expected.map(Row.fromSeq)
          afterEach()
      }
    }
  }
}

object YtSortedTableJoinProperties {
  case class JoinTest(source1: Source, source2: Source, joinColumns: Seq[String])

  private def genDividedSchema(schema: Schema, commonColumnsNumber: Int): Gen[(Schema, Schema)] = {
    for {
      schema1ColumnsNumber <- Gen.choose(0, schema.cols.length - commonColumnsNumber)
    } yield {
      (
        Schema(
          schema.cols.take(commonColumnsNumber) ++
          schema.cols.slice(commonColumnsNumber, commonColumnsNumber + schema1ColumnsNumber)
        ),
        Schema(
          schema.cols.take(commonColumnsNumber) ++
          schema.cols.slice(commonColumnsNumber + schema1ColumnsNumber, schema.cols.length)
        )
      )
    }
  }

  def genJoinTest: Gen[JoinTest] = {
    for {
      joinedSchema <- genSchema(3, 6)
      commonColumnsNumber <- Gen.choose(1, joinedSchema.cols.length)
      (schema1, schema2) <- genDividedSchema(joinedSchema, commonColumnsNumber)
      data1 <- genData(schema1)
      data2 <- genData(schema2)
      keysSchema1 <- genPrefixColumns(schema1)
      keysSchema2 <- genPrefixColumns(schema2)
    } yield {
      JoinTest(
        Source(schema1, keysSchema1, data1),
        Source(schema2, keysSchema2, data2),
        joinedSchema.getColumnNames(commonColumnsNumber)
      )
    }
  }

  def simulateJoin(data1: Seq[Seq[Any]], data2: Seq[Seq[Any]], joinColumnsNumber: Int): Seq[Seq[Any]] = {
    data1.flatMap {
      v1 =>
        val key = v1.take(joinColumnsNumber)
        val sameKey = data2.filter(_.take(joinColumnsNumber) == key)
        sameKey.map(v2 => v1 ++ v2.drop(joinColumnsNumber))
    }
  }
}
