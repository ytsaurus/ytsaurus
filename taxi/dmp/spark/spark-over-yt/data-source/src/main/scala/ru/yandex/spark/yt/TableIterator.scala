package ru.yandex.spark.yt

import ru.yandex.yt.ytclient.proxy.TableReader

class TableIterator[T](reader: TableReader[T]) extends Iterator[T] with AutoCloseable {
  var chunk: java.util.Iterator[T] = _

  override def hasNext: Boolean = {
    if (chunk != null && chunk.hasNext) {
      true
    } else if (reader.canRead) {
      reader.readyEvent().join()
      val list = reader.read()
      if (list != null) {
        chunk = list.iterator()
        chunk.hasNext || hasNext
      } else {
        reader.close().join() // TODO: add finally
        false
      }
    } else {
      reader.close().join() // TODO: add finally
      false
    }
  }

  override def next(): T = {
    chunk.next()
  }

  override def close(): Unit = {
    reader.close().join()
  }
}
