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
    .config("spark.hadoop.yt.proxy", "localhost:8000")
    .config("spark.hadoop.yt.user", "root")
    .config("spark.hadoop.yt.token", "")
    .config("spark.yt.timeout.minutes", "5")
    .config("spark.yt.write.step", "10")
    .config("spark.sql.sources.commitProtocolClass", "ru.yandex.spark.yt.format.YtOutputCommitter")
    .config("spark.ui.enabled", "false")
    .withExtensions(_.injectPlannerStrategy(_ => new YtSourceStrategy))
    .getOrCreate()
}
