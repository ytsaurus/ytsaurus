package ru.yandex.spark.yt.format.batch

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.annotation.tailrec

class EmptyColumnsBatchReaderTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "EmptyColumnsVectorizedReaderTest"

  it should "read row count batches" in {
    val table = Table(
      ("count", "expected"),
      (100L, Seq(100)),
      (Int.MaxValue.toLong + 100500, Seq(Int.MaxValue, 100500)),
      (3 * Int.MaxValue.toLong + 300, Seq(Int.MaxValue, Int.MaxValue, Int.MaxValue, 300)),
    )

    forAll(table) { (count: Long, expected: Seq[Int]) =>
      val reader = new EmptyColumnsBatchReader(count)
      readRowCount(reader) should contain theSameElementsInOrderAs expected
    }
  }

  def readRowCount(reader: EmptyColumnsBatchReader): Seq[Int] = {
    @tailrec
    def readIteration(res: Seq[Int]): Seq[Int] = {
      val batchRead = reader.nextBatch
      if (batchRead) {
        readIteration(reader.currentBatchSize +: res)
      } else res
    }

    readIteration(Nil).reverse
  }

}
