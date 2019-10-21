package ru.yandex.spark.test

import com.twitter.scalding.Args
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.yandex.spark.yt._
import org.apache.spark.sql.functions._
import ru.yandex.spark.yt.format.YtSourceStrategy


object Test extends App {
  try {
    val spark = SparkSession.builder()
      .config("spark.yt.user", sys.env("YT_SECURE_VAULT_YT_USER"))
      .config("spark.yt.token", sys.env("YT_SECURE_VAULT_YT_TOKEN"))
      .withExtensions(_.injectPlannerStrategy(_ => YtSourceStrategy))
      .getOrCreate()

    val user = spark.read.yt("/home/sashbel/data/user", 48)
    user.groupBy("application_platform").count().sort(desc("count"))
      .coalesce(1).write.mode(SaveMode.Overwrite).yt("/home/sashbel/data/test")
  } finally {
    YtClientProvider.close()
  }

}
