package tech.ytsaurus.spyt.wrapper.table

sealed trait TableType

object TableType {
  case object Static extends TableType

  case object Dynamic extends TableType
}
