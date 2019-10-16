package ru.yandex.spark.yt

import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.format.YtSourceStrategy
import ru.yandex.spark.yt.utils.DefaultRpcCredentials
import ru.yandex.yt.ytclient.proxy.YtClient

trait LocalSpark {
  val spark: SparkSession = LocalSpark.spark
  implicit val ytClient: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(spark))
}

object LocalSpark {
  private lazy val spark = SparkSession.builder()
    .master("local[2]")
    .config("fs.yt.impl", "ru.yandex.spark.yt.format.YtFileSystem")
    .config("fs.defaultFS", "yt:///")
    .config("spark.yt.proxy", "hume.yt.yandex.net")
    .config("spark.yt.user", DefaultRpcCredentials.user)
    .config("spark.yt.token", DefaultRpcCredentials.token)
    .config("spark.sql.files.maxPartitionBytes", "1000000")
    .config("spark.yt.timeout.minutes", "5")
    .config("spark.yt.write.step", "10")
    .config("spark.sql.sources.commitProtocolClass", "ru.yandex.spark.yt.format.YtOutputCommitter")
    .config("spark.task.maxFailures", "4")
    .withExtensions(_.injectPlannerStrategy(_ => YtSourceStrategy))
    .getOrCreate()
}
