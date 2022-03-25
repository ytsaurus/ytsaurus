package ru.yandex.spark.yt.logger

import org.apache.spark.TaskContext

case class TaskInfo(stageId: Int, partitionId: Int)

object TaskInfo {
  def apply(context: TaskContext): TaskInfo = {
    if (context == null) {
      TaskInfo(-1, -1)
    } else {
      TaskInfo(context.stageId(), context.partitionId())
    }
  }
}
