package ru.yandex.spark.yt.wrapper.table

sealed trait TableType

object TableType {
  case object Static extends TableType

  case object Dynamic extends TableType
}
