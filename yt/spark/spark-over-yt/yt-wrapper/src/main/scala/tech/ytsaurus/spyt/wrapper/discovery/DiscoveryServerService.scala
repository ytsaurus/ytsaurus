package tech.ytsaurus.spyt.wrapper.discovery

import org.slf4j.LoggerFactory
import tech.ytsaurus.client.DiscoveryClient
import tech.ytsaurus.client.discovery.MemberInfo
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import scala.util.Try

class DiscoveryServerService(client: DiscoveryClient, groupId: String,
                             masterGroupIdO: Option[String]) extends DiscoveryService {
  private val log = LoggerFactory.getLogger(getClass)

  private def addressAttr: String = "spark_address"

  private def webUiAttr: String = "webui"

  private def restAttr: String = "rest"

  private def operationAttr: String = "operation"

  private def livyAddressAttr: String = "livy_address"

  private def versionAttr: String = "version"

  private def confAttr: String = "conf"

  private def masterWrapperAttr: String = "master_wrapper"

  private def masterGroupId: String = masterGroupIdO.getOrElse(groupId)

  private def sendHeartbeat(groupId: String, memberInfo: MemberInfo): Unit = {
    try {
      DiscoveryClientWrapper.heartbeat(groupId, 2 * 60 * 1000 * 1000, memberInfo)(client)
    } catch {
      case e: Throwable => log.warn(s"Error while sending heartbeat for ${memberInfo.getId}", e)
    }
  }

  override def registerMaster(operationId: String,
                              address: Address,
                              clusterVersion: String,
                              masterWrapperEndpoint: HostAndPort,
                              clusterConf: SparkConfYsonable): Unit = {
    import scala.collection.JavaConverters._

    val confYTree = clusterConf.spark_conf.foldLeft(YTree.mapBuilder()) {
      case (builder, (k, v)) => builder.key(k).value(v)
    }.buildMap()
    val attributes: Map[String, YTreeNode] = Map(
      addressAttr -> YTree.stringNode(address.hostAndPort.toString),
      webUiAttr -> YTree.stringNode(address.webUiHostAndPort.toString),
      restAttr -> YTree.stringNode(address.restHostAndPort.toString),
      operationAttr -> YTree.stringNode(operationId),
      versionAttr -> YTree.stringNode(clusterVersion),
      masterWrapperAttr -> YTree.stringNode(masterWrapperEndpoint.toString),
      confAttr -> confYTree,
    )
    val memberInfo = new MemberInfo("master", 0L, 0L, attributes.asJava)
    sendHeartbeat(masterGroupId, memberInfo)
  }

  override def updateMaster(operationId: String,
                            address: Address,
                            clusterVersion: String,
                            masterWrapperEndpoint: HostAndPort,
                            clusterConf: SparkConfYsonable): Unit =
    registerMaster(operationId, address, clusterVersion, masterWrapperEndpoint, clusterConf)

  override def registerSHS(address: HostAndPort): Unit = ???

  override def registerLivy(address: HostAndPort, livyVersion: String): Unit = {
    import scala.collection.JavaConverters._

    val attributes: Map[String, YTreeNode] = Map(
      livyAddressAttr -> YTree.stringNode(address.toString),
      versionAttr -> YTree.stringNode(livyVersion),
    )
    val memberInfo = new MemberInfo("livy", 0L, 0L, attributes.asJava)
    sendHeartbeat(groupId, memberInfo)
  }

  override def updateLivy(address: HostAndPort, livyVersion: String): Unit = registerLivy(address, livyVersion)

  override def registerWorker(operationId: String): Unit = ???

  private def getMemberAddress(memberInfo: MemberInfo, attr: String): HostAndPort = {
    HostAndPort.fromString(memberInfo.getAttributes.get(attr).stringValue())
  }

  override def discoverAddress(): Try[Address] = {
    Try {
      val attrs = Seq(addressAttr, webUiAttr, restAttr)
      val members = DiscoveryClientWrapper.listMembers(masterGroupId, attrs = attrs)(client)
      val masterMember = members.find(_.getId == "master").get
      Address(
        getMemberAddress(masterMember, addressAttr),
        getMemberAddress(masterMember, webUiAttr),
        getMemberAddress(masterMember, restAttr),
      )
    }
  }

  override def operations(): Option[OperationSet] = ???

  override def masterWrapperEndpoint(): Option[HostAndPort] = ???

  override def operationInfo: Option[OperationInfo] = ???

  override def toString: String = s"DiscoveryServer[$groupId]"
}
