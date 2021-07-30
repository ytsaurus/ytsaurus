package ru.yandex.spark.yt.submit

import org.apache.spark.launcher.InProcessLauncher

object InProcessLauncherPythonUtils {
  def addAppArg(launcher: InProcessLauncher, arg: String): InProcessLauncher = {
    launcher.addAppArgs(arg)
  }
}
