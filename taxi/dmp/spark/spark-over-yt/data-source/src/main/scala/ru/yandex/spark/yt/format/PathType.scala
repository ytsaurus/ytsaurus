package ru.yandex.spark.yt.format

sealed trait PathType

object PathType {
  case object File extends PathType

  case object Table extends PathType

  case object Directory extends PathType

  case object None extends PathType
}
