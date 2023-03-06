package org.apache.spark.deploy.master

import org.apache.spark.SparkConf
import org.apache.spark.serializer.Serializer
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

class YtRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  override def createPersistenceEngine(): PersistenceEngine = {
    val yt = YtClientProvider.ytClient(ytClientConfiguration(conf))
    new YtPersistenceEngine(conf.get("spark.deploy.yt.path"), serializer)(yt)
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }

}
