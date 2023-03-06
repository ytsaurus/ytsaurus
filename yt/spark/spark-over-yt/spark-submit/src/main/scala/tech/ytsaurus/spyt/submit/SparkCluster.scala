package tech.ytsaurus.spyt.submit

import org.apache.spark.deploy.rest.RestSubmissionClientWrapper
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.discovery.CypressDiscoveryService

case class SparkCluster(master: String,
                        masterRest: String,
                        version: String,
                        client: RestSubmissionClientWrapper.Client,
                        masterHostAndPort: HostAndPort)

object SparkCluster {
  def get(proxy: String, discoveryPath: String, user: String, token: String): SparkCluster = {
    val ytClient = YtWrapper.createRpcClient("submission client", YtClientConfiguration.default(proxy, user, token))
    try {
      implicit val yt = ytClient.yt
      val discoveryService = new CypressDiscoveryService(discoveryPath + "/discovery")

      val address = discoveryService.discoverAddress().
        getOrElse(throw new IllegalArgumentException(s"Master address is not found, check discovery path $discoveryPath"))

      val clusterVersion = discoveryService.clusterVersion
        .getOrElse(throw new IllegalArgumentException(s"Cluster version is not found, check discovery path $discoveryPath"))

      val master = s"spark://${address.hostAndPort}"
      val rest = s"spark://${address.restHostAndPort}"
      val client = RestSubmissionClientWrapper.create(rest)

      SparkCluster(master, rest, clusterVersion, client, address.restHostAndPort)
    } finally {
      ytClient.close()
    }
  }
}
