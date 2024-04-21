package org.apache.spark.deploy.master

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import tech.ytsaurus.spyt.launcher.DeployMessages.{RegisterDriverToAppId, UnregisterDriverToAppId}
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@Decorate
@OriginClass("org.apache.spark.deploy.master.Master")
class MasterDecorators {

  @DecoratedMethod
  def org$apache$spark$deploy$master$Master$$removeDriver(
    driverId: String,
    finalState: DriverState,
    exception: Option[Exception]): Unit = {
    val completedDrivers = org$apache$spark$deploy$master$Master$$completedDrivers
    val before = new ArrayBuffer[DriverInfo]
    completedDrivers.copyToBuffer(before)

    __org$apache$spark$deploy$master$Master$$removeDriver(driverId, finalState, exception)

    MasterDecorators.checkAndRemoveDriverToAppId(self, before, completedDrivers)
  }

  def __org$apache$spark$deploy$master$Master$$removeDriver(
    driverId: String,
    finalState: DriverState,
    exception: Option[Exception]): Unit = ???

  @DecoratedMethod
  def org$apache$spark$deploy$master$Master$$createDriver(desc: DriverDescription): DriverInfo = {
    val dInfo = __org$apache$spark$deploy$master$Master$$createDriver(desc)
    val newDesc = dInfo.desc.copy(command =
      desc.command.copy(javaOpts = s"-Dspark.driverId=${dInfo.id}" +: desc.command.javaOpts))
    new DriverInfo(dInfo.startTime, dInfo.id, newDesc, dInfo.submitDate)
  }

  def __org$apache$spark$deploy$master$Master$$createDriver(desc: DriverDescription): DriverInfo = ???

  final def self: RpcEndpointRef = ???

  private val org$apache$spark$deploy$master$Master$$completedDrivers: mutable.ArrayBuffer[DriverInfo] = ???
}

object MasterDecorators extends Logging {
  def checkAndRemoveDriverToAppId(
    self: RpcEndpointRef,
    completedDriversBefore: mutable.ArrayBuffer[DriverInfo],
    completedDriversAfter: mutable.ArrayBuffer[DriverInfo]): Unit = {
    var i = 0
    while (i < completedDriversBefore.length && completedDriversBefore(i).id != completedDriversAfter.head.id) {
      val driverId = completedDriversBefore(i).id
      logInfo(s"Requesting to unregister driverId $driverId to app")
      self.send(UnregisterDriverToAppId(driverId))
      i += 1
    }
  }
}