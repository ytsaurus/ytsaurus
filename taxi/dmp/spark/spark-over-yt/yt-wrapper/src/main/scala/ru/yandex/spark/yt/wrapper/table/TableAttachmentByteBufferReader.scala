package ru.yandex.spark.yt.wrapper.table

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentRowsetReader

class TableAttachmentByteBufferReader extends TableAttachmentRowsetReader[ByteBuffer] {
  override protected def parseMergedRow(bb: ByteBuffer, size: Int): util.List[ByteBuffer] = {
    val res = bb.duplicate
    res.position(bb.position)
    res.limit(bb.position + size)
    bb.position(bb.position + size)
    Collections.singletonList(res)
  }
}
