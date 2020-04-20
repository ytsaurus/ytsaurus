package ru.yandex.spark.yt.wrapper.table

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

trait YtTableSettings {
  def ytSchema: YTreeNode

  def options: Map[String, String]
}
