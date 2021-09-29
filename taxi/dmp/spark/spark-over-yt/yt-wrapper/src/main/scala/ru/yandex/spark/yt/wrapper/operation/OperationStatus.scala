package ru.yandex.spark.yt.wrapper.operation

sealed abstract class OperationStatus(val isFinished: Boolean,
                                      val isSuccess: Boolean,
                                      val name: String)

object OperationStatus {
  case object Unknown extends OperationStatus(false, false, "unknown")

  case object Starting extends OperationStatus(false, false, "starting")

  case object Initializing extends OperationStatus(false, false, "initializing")

  case object Preparing extends OperationStatus(false, false, "preparing")

  case object Materializing extends OperationStatus(false, false, "materializing")

  case object Reviving extends OperationStatus(false, false, "reviving")

  case object ReviveInitializing extends OperationStatus(false, false, "revive_initializing")

  case object Orphaned extends OperationStatus(false, false, "orphaned")

  case object WaitingForAgent extends OperationStatus(false, false, "waiting_for_agent")

  case object Pending extends OperationStatus(false, false, "pending")

  case object Running extends OperationStatus(false, false, "running")

  case object Completing extends OperationStatus(false, false, "completing")

  case object Completed extends OperationStatus(true, true, "completed")

  case object Aborting extends OperationStatus(false, false, "aborting")

  case object Aborted extends OperationStatus(true, false, "aborted")

  case object Failing extends OperationStatus(false, false, "failing")

  case object Failed extends OperationStatus(true, false, "failed")

  def getByName(name: String): OperationStatus = {
    Seq(Unknown, Starting, Initializing, Preparing,
      Materializing, Reviving, ReviveInitializing,
      Orphaned, WaitingForAgent, Pending, Running,
      Completing, Completed, Aborting, Aborted, Failing)
      .find(_.name == name.toLowerCase)
      .getOrElse(throw new IllegalArgumentException(s"Unknown operation status $name"))
  }
}

