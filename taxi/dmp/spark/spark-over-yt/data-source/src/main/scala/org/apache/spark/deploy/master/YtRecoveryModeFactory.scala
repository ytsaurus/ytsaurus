package org.apache.spark.deploy.master

import org.apache.spark.SparkConf
import org.apache.spark.serializer.Serializer
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.wrapper.client.YtClientProvider

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
