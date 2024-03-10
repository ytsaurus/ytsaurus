package org.apache.spark.deploy.master

import org.apache.spark.deploy.master.Master.{ENDPOINT_NAME, SYSTEM_NAME}
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.internal.Logging
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEnv}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils => SparkUtils}
import tech.ytsaurus.spark.launcher.AddressUtils
import tech.ytsaurus.spyt.launcher.DeployMessages._
import tech.ytsaurus.spyt.Utils

import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

class YtMaster(rpcEnv: RpcEnv,
               address: RpcAddress,
               webUiPort: Int,
               securityMgr: SecurityManager,
               conf: SparkConf
              ) extends Master(rpcEnv, address, webUiPort, securityMgr, conf) {
  import YtMaster._

  private val driverIdToApp = new mutable.HashMap[String, String]
  private val baseClass = this.getClass.getSuperclass
  private val fieldsCache = new ConcurrentHashMap[String, Field]

  private def fieldOf(name: String): Field = {
    fieldsCache.computeIfAbsent(name, {fName =>
      val baseClassField = baseClass.getDeclaredField(fName)
      baseClassField.setAccessible(true)
      baseClassField
    })
  }

  private def getBaseClassFieldValue[T](field: BaseField[T]): T = {
    fieldOf(field.name).get(this).asInstanceOf[T]
  }

  private def setBaseClassFieldValue[T](field: BaseField[T], value: T): Unit = {
    fieldOf(field.name).set(this, value)
  }

  override def onStart(): Unit = {
    super.onStart()
    val webUi = getBaseClassFieldValue(WebUiField)
    val masterPublicAddress = getBaseClassFieldValue(MasterPublicAddressField)
    val restServerBoundPort = getBaseClassFieldValue(RestServerBoundPortField)
    val masterWebUiUrl = s"${webUi.scheme}${Utils.addBracketsIfIpV6Host(masterPublicAddress)}" +
      s":${webUi.boundPort}"
    setBaseClassFieldValue(MasterWebUiUrlField, masterWebUiUrl)
    AddressUtils.writeAddressToFile("master", masterPublicAddress,
      address.port, Some(webUi.boundPort), restServerBoundPort)
  }

  override def receive: PartialFunction[Any, Unit] = super.receive orElse {
    case RegisterDriverToAppId(driverId, appId) =>
      if (driverId != null && appId != null) {
        if (getBaseClassFieldValue(StateField) == RecoveryState.STANDBY) {
          // ignore, don't send response
        } else {
          logInfo("Registered driverId " + driverId + " to appId " + appId)
          driverIdToApp(driverId) = appId
        }
      } else {
        logInfo("Unsuccessful registration try " + driverId + " to " + appId)
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] =
    super.receiveAndReply(context) orElse {

    case RequestDriverStatuses =>
      val state = getBaseClassFieldValue(StateField)
      logDebug(s"Driver statuses requested, state=$state")
      if (state != RecoveryState.ALIVE) {
        val msg = s"${SparkUtils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver statuses in ALIVE state."
        context.reply(DriverStatusesResponse(Seq(), Some(new Exception(msg))))
      } else {
        val drivers = getBaseClassFieldValue(DriversField)
        val completedDrivers = getBaseClassFieldValue(CompletedDriversField)
        val statuses = (drivers ++ completedDrivers).map(driver =>
          DriverStatus(driver.id, driver.state.toString, driver.startTime))
        context.reply(DriverStatusesResponse(statuses.toSeq, None))
      }

    case RequestApplicationStatus(appId) =>
      val state = getBaseClassFieldValue(StateField)
      logDebug(s"Driver status requested, state=$state, id=$appId")
      if (state != RecoveryState.ALIVE) {
        context.reply(
          ApplicationStatusResponse(found = false, None))
      } else {
        logInfo("Asked application status for application " + appId)
        idToApp.get(appId) match {
          case Some(app) =>
            val appInfo = ApplicationInfo(app.id, app.state.toString, app.startTime, app.submitDate)
            context.reply(ApplicationStatusResponse(found = true, Some(appInfo)))
          case None =>
            context.reply(ApplicationStatusResponse(found = false, None))
        }
      }

    case RequestApplicationStatuses =>
      logInfo("Application statuses requested")
      val res = ApplicationStatusesResponse(
        idToApp.values.filter(app => app.state != ApplicationState.FINISHED).toSeq.map { app =>
          ApplicationInfo(app.id, app.state.toString, app.startTime, app.submitDate)
        },
        getBaseClassFieldValue(StateField) == RecoveryState.ALIVE
      )
      context.reply(res)

    case RequestAppId(driverId) =>
      logInfo("Asked app id for driver " + driverId)
      val appIdOption = driverIdToApp.get(driverId)
      context.reply(AppIdResponse(appIdOption))


  }

}

object YtMaster extends Logging {

  sealed trait BaseField[T] {
    val name: String
  }
  private case object StateField extends BaseField[RecoveryState.Value] {
    val name = "org$apache$spark$deploy$master$Master$$state"
  }
  private case object MasterWebUiUrlField extends BaseField[String] {
    val name = "org$apache$spark$deploy$master$Master$$masterWebUiUrl"
  }
  private case object WebUiField extends BaseField[MasterWebUI] {
    val name = "org$apache$spark$deploy$master$Master$$webUi"
  }
  private case object MasterPublicAddressField extends BaseField[String] { val name = "masterPublicAddress" }
  private case object RestServerBoundPortField extends BaseField[Option[Int]] {
    val name = "org$apache$spark$deploy$master$Master$$restServerBoundPort"
  }
  private case object DriversField extends BaseField[mutable.HashSet[DriverInfo]] {
    val name = "org$apache$spark$deploy$master$Master$$drivers"
  }
  private case object CompletedDriversField extends BaseField[mutable.ArrayBuffer[DriverInfo]] {
    val name = "org$apache$spark$deploy$master$Master$$completedDrivers"
  }

  def main(argStrings: Array[String]): Unit = {
    // almost exact copy of org.apache.spark.deploy.master.Master main method with slight changes
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    SparkUtils.initDaemon(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, args.host, args.port, conf, securityMgr)
    rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new YtMaster(rpcEnv, rpcEnv.address, args.webUiPort, securityMgr, conf))
    rpcEnv.awaitTermination()
  }
}