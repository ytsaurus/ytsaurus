package tech.ytsaurus.spyt.launcher

import java.util.Date

sealed trait DeployMessage extends Serializable

object DeployMessages {

  case class RegisterDriverToAppId(driverId: String, appId: String) extends DeployMessage

  case object RequestDriverStatuses extends DeployMessage

  case class RequestApplicationStatus(appId: String) extends DeployMessage

  case object RequestApplicationStatuses extends DeployMessage

  case class DriverStatus(id: String, state: String, startTimeMs: Long)

  case class DriverStatusesResponse(statuses: Seq[DriverStatus],
                                    exception: Option[Exception])

  case class ApplicationInfo(id: String, state: String, startTime: Long, submitDate: Date)

  case class ApplicationStatusResponse(found: Boolean, info: Option[ApplicationInfo])

  case class ApplicationStatusesResponse(statuses: Seq[ApplicationInfo], masterIsAlive: Boolean)

  case class RequestAppId(driverId: String) extends DeployMessage

  case class AppIdResponse(appId: Option[String])
}
