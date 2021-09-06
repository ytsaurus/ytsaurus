package ru.yandex.spark.yt.wrapper.operation

sealed abstract class OperationStatus(val isFinished: Boolean,
                                      val isSuccess: Boolean) {
  val name: String = {
    val className = this.getClass.getSimpleName.toLowerCase
    if (className.endsWith("$")) {
      className.dropRight(1)
    } else {
      className
    }
  }
}

object OperationStatus {
  case object Unknown extends OperationStatus(false, false)

  case object Starting extends OperationStatus(false, false)

  case object Initializing extends OperationStatus(false, false)

  case object Preparing extends OperationStatus(false, false)

  case object Materializing extends OperationStatus(false, false)

  case object Reviving extends OperationStatus(false, false)

  case object ReviveInitializing extends OperationStatus(false, false)

  case object Orphaned extends OperationStatus(false, false)

  case object WaitingForAgent extends OperationStatus(false, false)

  case object Pending extends OperationStatus(false, false)

  case object Running extends OperationStatus(false, false)

  case object Completing extends OperationStatus(false, false)

  case object Completed extends OperationStatus(true, true)

  case object Aborting extends OperationStatus(false, false)

  case object Aborted extends OperationStatus(true, false)

  case object Failing extends OperationStatus(false, false)

  case object Failed extends OperationStatus(true, false)

  def getByName(name: String): OperationStatus = {
    Seq(Unknown, Starting, Initializing, Preparing,
      Materializing, Reviving, ReviveInitializing,
      Orphaned, WaitingForAgent, Pending, Running,
      Completing, Completed, Aborting, Aborted, Failing)
      .find(_.name == name.toLowerCase)
      .getOrElse(throw new IllegalArgumentException(s"Unknown operation status $name"))
  }
}

