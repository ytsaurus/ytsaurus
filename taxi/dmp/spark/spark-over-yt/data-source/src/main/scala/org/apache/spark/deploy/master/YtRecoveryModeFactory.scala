package org.apache.spark.deploy.master

import org.apache.spark.SparkConf
import org.apache.spark.serializer.Serializer
import ru.yandex.spark.yt.{YtClientConfigurationConverter, YtClientProvider}
import ru.yandex.yt.ytclient.proxy.YtClient

class YtRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  override def createPersistenceEngine(): PersistenceEngine = {
    val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    new YtPersistenceEngine(conf.get("spark.deploy.yt.path"), serializer)(yt)
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }

}
