package ru.yandex.spark.yt

import ru.yandex.spark.yt.fs.GlobalTableSettings

object PythonUtils {
  def setPathFilesCount(path: String, filesCount: Int): Unit = {
    GlobalTableSettings.setFilesCount(path, filesCount)
  }
}
