package tech.ytsaurus.spyt.wrapper.dyntable

import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient}
import tech.ytsaurus.client.request.{AdvanceConsumer, PullConsumer, RegisterQueueConsumer, RowBatchReadOptions}
import tech.ytsaurus.client.rows.QueueRowset
import tech.ytsaurus.core.{DataSize, GUID}
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.wrapper.cypress.YtCypressUtils
import tech.ytsaurus.spyt.wrapper.transaction.YtTransactionUtils

import scala.annotation.tailrec

trait YtQueueUtils {
  self: YtCypressUtils with YtTransactionUtils =>

  def pullConsumer(consumerPath: String, queuePath: String, partitionIndex: Int, offset: Long, maxRowCount: Long)
                  (implicit yt: CompoundClient): QueueRowset = {
    val options = RowBatchReadOptions.builder()
      .setMaxRowCount(maxRowCount)
      .setMaxDataWeight(DataSize.fromTeraBytes(1)).build()
    val request = PullConsumer.builder()
      .setConsumerPath(YPath.simple(consumerPath))
      .setQueuePath(YPath.simple(queuePath))
      .setPartitionIndex(partitionIndex)
      .setOffset(offset)
      .setRowBatchReadOptions(options)
      .build()
    yt.pullConsumer(request).join()
  }

  def pullConsumerStrict(consumerPath: String, queuePath: String, partitionIndex: Int, offset: Long, rowCount: Long)
                  (implicit yt: CompoundClient): Seq[QueueRowset] = {
    @tailrec
    def inner(lowerIndex: Long, size: Long, result: List[QueueRowset]): Seq[QueueRowset] = {
      val queueRowset = pullConsumer(consumerPath, queuePath, partitionIndex, lowerIndex, size)
      val newResult = queueRowset :: result
      val receivedRows = queueRowset.getRows.size()
      if (receivedRows == size) {
        newResult
      } else if (receivedRows == 0) {
        throw new IllegalStateException(s"Read no rows," +
          s" but $size rows starting from index $lowerIndex were requested in partition $partitionIndex")
      } else {
        inner(lowerIndex + receivedRows, size - receivedRows, newResult)
      }
    }
    inner(offset, rowCount, Nil)
  }

  def advanceConsumer(consumerPath: YPath, queuePath: YPath, partitionIndex: Int, newOffset: Long,
                      transaction: ApiServiceTransaction): Unit = {
    val request = AdvanceConsumer.builder()
      .setConsumerPath(consumerPath)
      .setQueuePath(queuePath)
      .setPartitionIndex(partitionIndex)
      .setNewOffset(newOffset)
      .build()
    transaction.advanceConsumer(request).join()
  }

  def registerQueueConsumer(consumerPath: YPath, queuePath: YPath, vital: Boolean = true)
                           (implicit yt: CompoundClient): Unit = {
    val request = RegisterQueueConsumer.builder()
      .setConsumerPath(consumerPath)
      .setQueuePath(queuePath)
      .setVital(vital)
      .build()
    yt.registerQueueConsumer(request).join()
  }
}
