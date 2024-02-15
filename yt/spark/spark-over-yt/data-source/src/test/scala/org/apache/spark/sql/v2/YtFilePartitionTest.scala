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
}
