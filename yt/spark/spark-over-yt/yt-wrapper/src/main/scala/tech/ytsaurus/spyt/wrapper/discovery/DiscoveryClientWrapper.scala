package tech.ytsaurus.spyt.wrapper.discovery

import org.slf4j.LoggerFactory
import tech.ytsaurus.client.DiscoveryClient
import tech.ytsaurus.client.discovery.{Heartbeat, ListMembers, ListMembersOptions, MemberInfo}

object DiscoveryClientWrapper {
  private val log = LoggerFactory.getLogger(getClass)

  def heartbeat(groupId: String, leaseTimeout: Long, memberInfo: MemberInfo)
               (implicit client: DiscoveryClient): Unit = {
    log.debug(s"Heartbeat: ${memberInfo.getId}, groupId: $groupId")
    val heartbeatReq = Heartbeat.builder()
      .setGroupId(groupId)
      .setLeaseTimeout(leaseTimeout)
      .setMemberInfo(memberInfo).build()
    client.heartbeat(heartbeatReq).join()
  }

  def listMembers(groupId: String, limit: Int = 20, attrs: Seq[String] = Seq.empty)
                 (implicit client: DiscoveryClient): Seq[MemberInfo] = {
    import scala.collection.JavaConverters._

    log.debug(s"List members: $groupId")
    val listMembersReq = ListMembers.builder()
      .setGroupId(groupId)
      .setOptions(new ListMembersOptions(limit, attrs.asJava))
      .build()
    client.listMembers(listMembersReq).join().getMembers.asScala
  }
}
