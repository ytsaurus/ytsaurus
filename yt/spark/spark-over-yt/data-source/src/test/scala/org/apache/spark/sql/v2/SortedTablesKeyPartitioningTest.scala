package org.apache.spark.sql.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.v2.Utils.{extractRawKeys, extractYtScan, getParsedKeys}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.common.utils.{TuplePoint, TupleSegment}
import tech.ytsaurus.spyt.format.YtInputSplit
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.serializers.PivotKeysConverter
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TmpDir}
import tech.ytsaurus.spyt.{YtReader, YtWriter}
import tech.ytsaurus.spyt.common.utils.{MInfinity, PInfinity, RealValue}
import tech.ytsaurus.spyt.format.YtPartitionedFile

import java.sql.{Date, Timestamp}
import java.time.LocalDate

class SortedTablesKeyPartitioningTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with MockitoSugar with DynTableTestUtils {
  behavior of "YtScan"

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}", value = true)
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}", value = 2)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}", value = false)
  }

  private def validatePivotKeys(keys: Seq[(TuplePoint, TuplePoint)]): Unit = {
    keys.head._1 shouldBe TupleSegment.mInfinity
    keys.zip(keys.tail).foreach { case ((_, prevSegmentEnd), (nextSegmentBegin, _)) =>
      prevSegmentEnd.points.foreach(_.isInstanceOf[RealValue[_]] shouldBe true)
      prevSegmentEnd shouldBe nextSegmentBegin
    }
    keys.last._2 shouldBe TupleSegment.pInfinity
  }

  it should "read any subset of columns" in {
    val cols = Seq("a", "b", "c", "d")
    val colIndexes = cols.indices
    val data = (0 until 1000).map(x => (x / 100, x / 10, x, -x))
    data.toDF(cols : _*).write.sortedBy("a", "b", "c").yt(tmpPath)

    // all subsets of columns
    colIndexes.toIterator.flatMap(i => colIndexes.combinations(i)).foreach {
      subColIndexes =>
        val subCols = subColIndexes.map(x => cols(x))
        val res = spark.read.yt(tmpPath).select(subCols.map(col) : _*).collect()
        val answer = data.map(x => subColIndexes.map(i => x.productElement(i)))
        res should contain theSameElementsAs answer.map(Row.fromSeq)
    }
  }

  it should "support parallel partition discovery" in {
    val testThresholds = Seq(1, 10, 100, 1000)

    val data = (0 until 1000).map(x => (x / 100, x / 10, x, -x))
    data.toDF("a", "b", "c", "d").write.sortedBy("a", "b").yt(tmpPath)

    testThresholds.foreach {
      threshold =>
        withConf("spark.sql.sources.parallelPartitionDiscovery.threshold", threshold.toString) {
          val res = spark.read.yt(tmpPath).collect()
          res should contain theSameElementsAs data.map(Row.fromTuple)
        }
    }
  }

  it should "work with predicate pushdown" in {
    withConf(SparkYtConfiguration.Read.KeyColumnsFilterPushdown.Enabled, true) {
      val data = (1L to 10000L).map(x => (x, x % 2))
      val df = data.toDF("a", "b")

      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath)
      val test = Seq(
        (
          (res("a") <= 50 && res("a") >= 49) && res("b") === 1L,
          data.filter { case (a, b) => a >= 49 && a <= 50 && b == 1 }
        ), (
          res("a") >= 77L && res("b").isin(0L),
          data.filter { case (a, b) => a >= 77 && b == 0 }
        ), (
          res("a") === 1L || res("b") === 2L,
          data.filter { case (a, b) => a == 1 || b == 2 }
        )
      )
      test.foreach {
        case (input, output) =>
          res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
      }
    }
  }

  def getParsedKeysByPath(paths: String*): Seq[(TuplePoint, TuplePoint)] = {
    getParsedKeys(spark.read.yt(paths: _*))
  }

  private val partitionConf = Map("spark.sql.files.maxPartitionBytes"-> "512", "spark.yt.minPartitionBytes" -> "512")

  it should "be satisfied by key partitioning" in {
    val data = (0 until 100).map(x => (x, x, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(partitionConf) {
      val keys = getParsedKeysByPath(tmpPath)
      keys should contain theSameElementsAs Seq(
        (TuplePoint(Seq(MInfinity())), TuplePoint(Seq(RealValue(25)))),
        (TuplePoint(Seq(RealValue(25))), TuplePoint(Seq(RealValue(50)))),
        (TuplePoint(Seq(RealValue(50))), TuplePoint(Seq(RealValue(75)))),
        (TuplePoint(Seq(RealValue(75))), TuplePoint(Seq(PInfinity())))
      )
    }
  }

  it should "be satisfied by splitting by 2 columns" in {
    val data = (0 until 100).map(x => (x / 100, x, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(Map("spark.sql.files.maxPartitionBytes"-> "320", "spark.yt.minPartitionBytes" -> "320")) {
      val keys = getParsedKeysByPath(tmpPath)
      keys should contain theSameElementsAs Seq(
        (TuplePoint(Seq(MInfinity())), TuplePoint(Seq(RealValue(0), RealValue(25)))),
        (TuplePoint(Seq(RealValue(0), RealValue(25))), TuplePoint(Seq(RealValue(0), RealValue(50)))),
        (TuplePoint(Seq(RealValue(0), RealValue(50))), TuplePoint(Seq(RealValue(0), RealValue(75)))),
        (TuplePoint(Seq(RealValue(0), RealValue(75))), TuplePoint(Seq(PInfinity())))
      )
    }
  }

  it should "not be satisfied by using key partitioning because of many merged partitions" in {
    val data = (0 until 200).map(x => (x / 200, x / 200, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(partitionConf) {
      val readTask = spark.read.yt(tmpPath)
      readTask.collect()
      val ytScan = extractYtScan(readTask.queryExecution.executedPlan)
      ytScan.tryKeyPartitioning() shouldBe None
    }
  }

  it should "work with boolean types" in {
    val data = (0 until 100).map(x => (x >= 50, -x))
    data.toDF("a", "b").write.sortedBy("a").yt(tmpPath)

    withConfs(Map("spark.sql.files.maxPartitionBytes"-> "320", "spark.yt.minPartitionBytes" -> "320")) {
      val keys = getParsedKeysByPath(tmpPath)
      keys should contain theSameElementsAs Seq(
        (TuplePoint(Seq(MInfinity())), TuplePoint(Seq(RealValue(false)))),
        (TuplePoint(Seq(RealValue(false))), TuplePoint(Seq(RealValue(true)))),
        (TuplePoint(Seq(RealValue(true))), TuplePoint(Seq(PInfinity())))
      )
    }
  }

  it should "merge partitions" in {
    val data = (0 until 100).map(x => (x / 49, x, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(partitionConf) {
      val keys = getParsedKeysByPath(tmpPath)
      keys.length should be > 2
      validatePivotKeys(keys)
    }
  }

  it should "process empty table" in {
    val data = Seq.empty[(Int, Int, Int)]
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(partitionConf) {
      val keys = getParsedKeysByPath(tmpPath)
      keys should contain theSameElementsAs Seq()
    }
  }

  it should "process small dataset" in {
    val data = Seq((0, 0, 0))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    val keys = getParsedKeysByPath(tmpPath)
    keys should contain theSameElementsAs Seq(
      (TupleSegment.mInfinity, TupleSegment.pInfinity)
    )
  }

  it should "process many partitions" in {
    val data = (0 until 10000).map(x => (x / 256, x, -x))
    data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)

    withConfs(Map("spark.sql.files.maxPartitionBytes"-> "1024", "spark.yt.minPartitionBytes" -> "1024")) {
      val keys = getParsedKeysByPath(tmpPath)
      keys.length should be > 10
      validatePivotKeys(keys)
    }
  }

  it should "fail reading few tables" in {
    Seq((0, 0, 0)).toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)
    Seq((1, 0, 0)).toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath + "2")

    val keys = getParsedKeysByPath(tmpPath, tmpPath + "2")
    keys should contain theSameElementsAs Seq(
      (TupleSegment.mInfinity, TupleSegment.pInfinity),
      (TupleSegment.mInfinity, TupleSegment.pInfinity)
    )
  }

  it should "group neighboring elements" in {
    val data1 = Seq((1, 1), (1, 2), (2, 3), (3, 4), (2, 5), (2, 6))
    YtFilePartition.seqGroupBy(data1) shouldBe Seq(
      (1, Seq(1, 2)), (2, Seq(3)), (3, Seq(4)), (2, Seq(5, 6))
    )

    val data2 = Seq(("1", 1), ("1", 2), (null, 2), ("2", 3), ("3", 4), ("2", 5), ("2", 6))
    YtFilePartition.seqGroupBy(data2) shouldBe Seq(
      ("1", Seq(1, 2)), (null, Seq(2)), ("2", Seq(3)), ("3", Seq(4)), ("2", Seq(5, 6))
    )
  }

  it should "support null keys" in {
    val data = Seq((null, 0), (null, 1), ("b", 2))
    data.toDF("a", "b").write.sortedBy("a").yt(tmpPath)

    withConfs(Map("spark.sql.files.maxPartitionBytes"-> "1", "spark.yt.minPartitionBytes" -> "1")) {
      val keys = getParsedKeysByPath(tmpPath)
      keys should contain theSameElementsAs Seq(
        (TuplePoint(Seq(MInfinity())), TuplePoint(Seq(RealValue(null)))),
        (TuplePoint(Seq(RealValue(null))), TuplePoint(Seq(RealValue("b")))),
        (TuplePoint(Seq(RealValue("b"))), TuplePoint(Seq(PInfinity())))
      )
    }
  }

  it should "get pivot keys" in {
    val data = Seq("a", "b", "c", "d", "e", "f")
    data.toDF("a").write.sortedBy("a").yt(tmpPath)

    val schema = StructType(Seq(StructField("a", StringType)))
    val files = Seq(
      YtPartitionedFile.static(tmpPath, 0, 1, 0),
      YtPartitionedFile.static(tmpPath, 2, 3, 0),
      YtPartitionedFile.static(tmpPath, 3, 4, 0),
      YtPartitionedFile.static(tmpPath, 5, 6, 0),
    )

    val res = YtFilePartition.getPivotKeys(schema, Seq("a"), files)
    res should contain theSameElementsAs Seq(
      TuplePoint(Seq(MInfinity())),
      TuplePoint(Seq(RealValue("c"))),
      TuplePoint(Seq(RealValue("d"))),
      TuplePoint(Seq(RealValue("f")))
    )
  }

  it should "add keys to partitioned files" in {
    val pivotKeys = Seq(
      TuplePoint(Seq(RealValue("a"))),
      TuplePoint(Seq(RealValue("c"))),
      TuplePoint(Seq(RealValue("d"))),
      TuplePoint(Seq(PInfinity()))
    )
    val files = Seq(
      YtPartitionedFile.static(tmpPath, 0, 1, 0, 0, null),
      YtPartitionedFile.static(tmpPath, 2, 3, 0, 0, null),
      YtPartitionedFile.static(tmpPath, 3, 4, 0, 0, null),
      YtPartitionedFile.static(tmpPath, 5, 6, 0, 0, null)
    )
    val filesGroupedByPoint = Seq(
      (pivotKeys(0), Seq(files(0))),
      (pivotKeys(1), Seq(files(1))),
      (pivotKeys(2), Seq(files(2), files(3)))
    )

    val res = extractRawKeys(YtFilePartition.getFilesWithUniquePivots(Seq("a"), filesGroupedByPoint))
    res should contain theSameElementsAs Seq(
      (Some(pivotKeys(0)), Some(pivotKeys(1))),
      (Some(pivotKeys(1)), Some(pivotKeys(2))),
      (Some(pivotKeys(2)), Some(pivotKeys(3)))
    )
  }

  it should "work on dynamic table" in {
    val tmpPath2 = tmpPath + "dynTable1"
    prepareTestTable(tmpPath2, testData, Seq(Seq(), Seq(3, 2), Seq(3, 3), Seq(3, 4)))

    val keys = getParsedKeysByPath(tmpPath2)
    keys should contain theSameElementsAs Seq(
      (TupleSegment.mInfinity, TuplePoint(Seq(RealValue(3), RealValue(2)))),
      (TuplePoint(Seq(RealValue(3), RealValue(2))), TuplePoint(Seq(RealValue(3), RealValue(3)))),
      (TuplePoint(Seq(RealValue(3), RealValue(3))), TuplePoint(Seq(RealValue(3), RealValue(4)))),
      (TuplePoint(Seq(RealValue(3), RealValue(4))), TupleSegment.pInfinity)
    )
  }

  it should "work on dynamic table with merging pivots" in {
    val tmpPath2 = tmpPath + "dynTable2"
    prepareTestTable(tmpPath2, testData, Seq(Seq(), Seq(3, 2), Seq(3, 3), Seq(6)))

    val keys = getParsedKeysByPath(tmpPath2)
    keys should contain theSameElementsAs Seq(
      (TupleSegment.mInfinity, TuplePoint(Seq(RealValue(3)))),
      (TuplePoint(Seq(RealValue(3))), TuplePoint(Seq(RealValue(6)))),
      (TuplePoint(Seq(RealValue(6))), TupleSegment.pInfinity)
    )
  }
}
