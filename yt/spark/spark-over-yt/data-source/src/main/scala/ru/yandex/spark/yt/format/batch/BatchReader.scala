package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.vectorized.ColumnarBatch

trait BatchReader extends AutoCloseable {
  def currentBatch: ColumnarBatch

  def nextBatch: Boolean

  def currentBatchSize: Int
}
