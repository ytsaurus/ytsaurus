package ru.yandex.spark.e2e.check

class GroupedIterator[T](iter: Iterator[T])
                        (key: T => Any) extends Iterator[Iterator[T]] {
  private var current: T = _
  private var currentActual = false

  private def nextCurrent: T = {
    current = iter.next()
    currentActual = true
    current
  }

  override def hasNext: Boolean = iter.hasNext


  override def next(): Iterator[T] = {
    if (currentActual) {
      new GroupIterator(key(current))
    } else {
      new GroupIterator(key(nextCurrent))
    }
  }

  class GroupIterator(groupKey: Any) extends Iterator[T] {
    override def hasNext: Boolean = {
      currentActual || iter.hasNext && key(nextCurrent) == groupKey
    }

    override def next(): T = {
      currentActual = false
      current
    }
  }
}

object GroupedIterator {
  implicit class RichIterator[T](iter: Iterator[T]) {
    def groupedBy(key: T => Any): GroupedIterator[T] = {
      new GroupedIterator[T](iter)(key)
    }
  }
}
