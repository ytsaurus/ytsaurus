package tech.ytsaurus.spyt.wrapper.table

trait PagedArrowInputStream {
  def isNextPage: Boolean

  def isEmptyPage: Boolean
}
