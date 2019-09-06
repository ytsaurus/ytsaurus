package ru.yandex.spark.yt

import org.apache.spark.sql.SparkSession

trait LocalSpark {
  val spark: SparkSession = LocalSpark.spark
}

object LocalSpark {
  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .config("fs.yt.impl", "ru.yandex.spark.yt.file.YtFileSystem")
    .config("yt.proxy", "hume")
    .config("yt.user", DefaultRpcCredentials.user)
    .config("yt.token", DefaultRpcCredentials.token)
    .config("spark.sql.files.maxPartitionBytes", "1000000")
    .config("spark.yt.timeout.minutes", "5")
    .config("spark.yt.write.step", "10")
    .getOrCreate()
}
