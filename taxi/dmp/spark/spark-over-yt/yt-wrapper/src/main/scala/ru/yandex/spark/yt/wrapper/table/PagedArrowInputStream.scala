package ru.yandex.spark.yt.wrapper.table

trait PagedArrowInputStream {
  def isNextPage: Boolean
}
