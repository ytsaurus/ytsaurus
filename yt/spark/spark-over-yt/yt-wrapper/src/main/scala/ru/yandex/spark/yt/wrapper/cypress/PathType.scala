package ru.yandex.spark.yt.wrapper.cypress

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

sealed trait PathType

object PathType {
  case object File extends PathType

  case object Table extends PathType

  case object Directory extends PathType

  case object None extends PathType

  def fromString(attr: String): PathType = {
    attr match {
      case "file" => PathType.File
      case "table" => PathType.Table
      case "map_node" => PathType.Directory
      case _ => PathType.None
    }
  }

  def fromAttributes(attributes: Map[String, YTreeNode]): PathType = {
    fromString(attributes(YtAttributes.`type`).stringValue())
  }
}
