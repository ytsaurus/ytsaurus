package tech.ytsaurus.spyt.wrapper.discovery
import tech.ytsaurus.spyt.HostAndPort

import scala.concurrent.duration.Duration
import scala.util.Try

class CompoundDiscoveryService(services: Seq[DiscoveryService]) extends DiscoveryService {
  if (services.isEmpty) {
    throw new IllegalArgumentException("No discovery services provided")
  }

  override def registerMaster(operationId: String,
                              address: Address,
                              clusterVersion: String,
                              masterWrapperEndpoint: HostAndPort,
                              clusterConf: SparkConfYsonable): Unit =
    services.foreach(_.registerMaster(operationId, address, clusterVersion, masterWrapperEndpoint, clusterConf))

  override def updateMaster(operationId: String,
                            address: Address,
                            clusterVersion: String,
                            masterWrapperEndpoint: HostAndPort,
                            clusterConf: SparkConfYsonable): Unit =
    services.foreach(_.updateMaster(operationId, address, clusterVersion, masterWrapperEndpoint, clusterConf))

  override def registerSHS(address: HostAndPort): Unit = services.foreach(_.registerSHS(address))

  override def registerLivy(address: HostAndPort, livyVersion: String): Unit =
    services.foreach(_.registerLivy(address, livyVersion))

  override def updateLivy(address: HostAndPort, livyVersion: String): Unit =
    services.foreach(_.updateLivy(address, livyVersion))

  override def registerWorker(operationId: String): Unit = services.foreach(_.registerWorker(operationId))

  override def discoverAddress(): Try[Address] = services.head.discoverAddress()

  override def operations(): Option[OperationSet] = services.head.operations()

  override def masterWrapperEndpoint(): Option[HostAndPort] = services.head.masterWrapperEndpoint()

  override def waitAddress(timeout: Duration): Option[Address] = services.head.waitAddress(timeout)

  override def operationInfo: Option[OperationInfo] = services.head.operationInfo

  override def toString: String = s"CompoundDiscovery[${services.map(_.toString).mkString(", ")}]"
}
