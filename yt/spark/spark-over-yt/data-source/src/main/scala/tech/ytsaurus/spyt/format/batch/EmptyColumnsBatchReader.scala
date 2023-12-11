package tech.ytsaurus.spyt.format.batch


class EmptyColumnsBatchReader(totalRowCount: Long) extends BatchReaderBase {
  override protected def nextBatchInternal: Boolean = {
    val num = Math.min(totalRowCount - _rowsReturned, Int.MaxValue).toInt
    setNumRows(num)
    num > 0
  }

  override protected def finalRead(): Unit = {}

  override def close(): Unit = {}
}
