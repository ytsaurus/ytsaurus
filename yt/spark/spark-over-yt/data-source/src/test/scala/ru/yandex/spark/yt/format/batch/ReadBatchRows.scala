package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{Matchers, TestSuite}

import scala.annotation.tailrec

trait ReadBatchRows {
  self: TestSuite with Matchers =>

  def readFully(reader: BatchReader, schema: StructType, batchMaxSize: Int): Seq[Row] = {
    import scala.collection.JavaConverters._
    @tailrec
    def readIteration(rows: Seq[Row]): Seq[Row] = {
      val batchRead = reader.nextBatch
      if (batchRead) {
        val (newRows, batchSize) = reader.currentBatch
          .rowIterator().asScala
          .foldLeft((rows, 0)) { case ((res, count), next) =>
            val row = Row.fromSeq(next.toSeq(schema))
            (row +: res, count + 1)
          }
        batchSize shouldEqual reader.currentBatchSize
        batchSize should be <= batchMaxSize
        readIteration(newRows)
      } else rows
    }

    readIteration(Nil).reverse
  }
}
