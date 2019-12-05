package ru.yandex.spark.yt.utils

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

trait YtTableSettings {
  def ytSchema: YTreeNode

  def options: Map[String, String]
}
