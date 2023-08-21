package org.apache.spark.sql.v2

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class YtFilePartitionTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "YtFilePartitionTest"

  import TestPartitionedFile._

  it should "order partitionedFiles" in {
    val expected = Seq(
      Dynamic("//path0", 1), // ordered by path

      Static("//path1", 0, 10), // grouped by path and ordered by beginRow
      Static("//path1", 10, 20),
      Static("//path1", 30, 40),

      Static("//path2", 0, 100), // grouped by path and ordered by beginRow
      Static("//path2", 100, 200),

      Dynamic("//path3", 2), // ordered by path

      Csv("//path4", 5), // ordered by length descending
      Csv("//path5", 4)
    )

    val actual = Random.shuffle(expected.map(_.toPartitionedFile))
      .sorted(YtFilePartition.partitionedFilesOrdering)
      .map(fromPartitionedFile)

    actual should contain theSameElementsInOrderAs expected
  }

  it should "merge static table's files" in {
    val inputFull = Seq(
      // 0
      Dynamic("//path0", 1),
      // 1
      Static("//path1", 0, 10),
      Static("//path1", 10, 20),
      // 3
      Static("//path1", 30, 40),
      // 4
      Static("//path2", 0, 100),
      Static("//path2", 100, 200),
      // 6
      Dynamic("//path3", 2),
      // 7
      Csv("//path4", 5),
      // 8
      Csv("//path5", 4)
      // 9
    )
    val expectedFull = Seq(
      Dynamic("//path0", 1),
      Static("//path1", 0, 20),
      Static("//path1", 30, 40),
      Static("//path2", 0, 200),
      Dynamic("//path3", 2),
      Csv("//path4", 5),
      Csv("//path5", 4)
    )
    val indices = Seq(0, 1, 3, 4, 6, 7, 8, 9).zipWithIndex
    val table = Table(
      ("input", "expected"),
      indices.map { case (inputIndex, expectedIndex) =>
        (inputFull.take(inputIndex), expectedFull.take(expectedIndex))
      }: _*
    )

    forAll(table) { (input: Seq[TestPartitionedFile], expected: Seq[TestPartitionedFile]) =>
      val res = YtFilePartition.mergeFiles(input.map(_.toPartitionedFile)).map(fromPartitionedFile)
      res should contain theSameElementsInOrderAs expected
    }
  }
}
