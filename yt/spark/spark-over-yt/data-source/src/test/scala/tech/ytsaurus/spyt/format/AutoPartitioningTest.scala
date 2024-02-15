package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition
import org.apache.spark.sql.internal.SQLConf.{FILES_MAX_PARTITION_BYTES, FILES_OPEN_COST_IN_BYTES}
import org.apache.spark.sql.v2.TestPartitionedFile
import org.apache.spark.{Partition, Partitioner}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.fs.conf.YT_MIN_PARTITION_BYTES
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.test.{DynTableTestUtils, TestRow}

import scala.util.Random

class AutoPartitioningTest extends FlatSpec with Matchers with LocalSpark with TmpDir with DynTableTestUtils {
  behavior of "YtDataSource"

  import TestPartitionedFile._
  import spark.implicits._

  it should "split large chunks in reading plan for static table" in {
    val data = 1 to 10
    data.toDF().coalesce(1).write.yt(tmpPath)

    val (partitions, res) = withConf(FILES_MAX_PARTITION_BYTES, "4B") {
      withConf(YT_MIN_PARTITION_BYTES, "4B", Some("1Gb")) {
        val df = spark.read.yt(tmpPath)
        (df.rdd.partitions, df.as[Long].collect())
      }
    }

    partitions.length shouldEqual 10
    getPartitionsFiles(partitions) should contain theSameElementsAs (0 until 10).map( i =>
      Seq(Static(tmpPath, i, i + 1)),
    )
    res should contain theSameElementsAs data
  }

  it should "merge small chunks in reading plan for static table" in {
    val data = 1 to 100
    repartition(data.toDS(), 100).write.yt(tmpPath)

    val (partitions, res) = withConf(FILES_OPEN_COST_IN_BYTES, "0") {
      withConf(YT_MIN_PARTITION_BYTES, "0B", Some("1Gb")) {
        val df = spark.read.yt(tmpPath)
        (df.rdd.partitions, df.as[Long].collect())
      }
    }

    partitions.length should be < 10
    res should contain theSameElementsAs data
  }

  it should "merge small chunks if default parallelism partition size is less than YT_MIN_PARTITION_BYTES" in {
    val data = 1 to 100
    repartition(data.toDS(), 100).write.yt(tmpPath)

    val (partitions, res) = withConf(FILES_OPEN_COST_IN_BYTES, "0") {
      val df = spark.read.yt(tmpPath)
      (df.rdd.partitions, df.as[Long].collect())
    }

    partitions.length shouldEqual 1
    getPartitionsFiles(partitions) should contain theSameElementsAs Seq(
      Seq(Static(tmpPath, 0, 100))
    )
    res should contain theSameElementsAs data
  }

  it should "not merge chunks from different static tables" in {
    YtWrapper.createDir(tmpPath)
    val data1 = 1 to 60
    val data2 = 61 to 100
    repartition(data1.toDS(), 60).write.yt(s"$tmpPath/1")
    repartition(data2.toDS(), 40).write.yt(s"$tmpPath/2")

    val (partitions, res) = withConf(FILES_OPEN_COST_IN_BYTES, "0") {
      withConf(YT_MIN_PARTITION_BYTES, "0B", Some("1Gb")) {
        val df = spark.read.yt(tmpPath)
        (df.rdd.partitions, df.as[Long].collect())
      }
    }

    partitions.length should be >= 2
    res should contain theSameElementsAs (data1 ++ data2)
  }

  it should "merge small chunks in reading plan for dynamic table" ignore {
    val r = new Random()
    val testData = (1 to 1000).map(i => TestRow(i, i * 2, r.nextString(10)))
    val pivotKeys = Seq() +: (10 until 1000 by 10).map(i => Seq(i))
    prepareTestTable(tmpPath, testData, pivotKeys)

    val (partitions, res) = withConf(FILES_OPEN_COST_IN_BYTES, "1") {
      withConf(YT_MIN_PARTITION_BYTES, "0B", Some("1Gb")) {
        val df = spark.read.yt(tmpPath)
        (df.rdd.partitions, df.as[TestRow].collect())
      }
    }

    partitions.length shouldEqual defaultParallelism +- 1
    res should contain theSameElementsAs testData
  }

  it should "merge small chunks if default parallelism partition size is less than YT_MIN_PARTITION_BYTES for dynamic table" ignore {
    val r = new Random()
    val testData = (1 to 1000).map(i => TestRow(i, i * 2, r.nextString(10)))
    val pivotKeys = Seq() +: (10 until 1000 by 10).map(i => Seq(i))
    prepareTestTable(tmpPath, testData, pivotKeys)

    val (partitions, res) = withConf(FILES_OPEN_COST_IN_BYTES, "1") {
        val df = spark.read.yt(tmpPath)
        (df.rdd.partitions, df.as[TestRow].collect())
    }

    partitions.length shouldEqual 1
    res should contain theSameElementsAs testData
  }

  it should "not split large chunks in reading plan for dynamic table" in {
    prepareTestTable(tmpPath, testData, Nil)

    val (partitions, res) = withConf(FILES_MAX_PARTITION_BYTES, "4B") {
      val df = spark.read.yt(tmpPath)
      (df.rdd.partitions, df.as[TestRow].collect())
    }

    partitions.length shouldEqual 1
    res should contain theSameElementsAs testData
  }

  def getPartitionsFiles(partitions: Seq[Partition]): Seq[Seq[TestPartitionedFile]] = {
    partitions.map { part =>
      part.asInstanceOf[DataSourceRDDPartition]
        .inputPartition.asInstanceOf[FilePartition]
        .files.toSeq
        .map(fromPartitionedFile)
    }
  }

  def repartition(df: Dataset[Int], partitionsNum: Int): Dataset[Int] = {
    val rdd = df.rdd.map(x => (x, x))
    val partitionedRdd = rdd.partitionBy(new Partitioner {
      override def numPartitions: Int = partitionsNum

      override def getPartition(key: Any): Int = {
        key match {
          case x: Int => x % numPartitions
        }
      }
    })
    partitionedRdd.map(_._1).toDF().as[Int]
  }

}
