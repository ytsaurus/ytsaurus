name := "scala-examples"

version := "0.1"

scalaVersion := "2.12.16"

resolvers += ("YandexMediaReleases" at "http://artifactory.yandex.net/artifactory/yandex_media_releases")
    .withAllowInsecureProtocol(true)

resolvers += ("YandexSparkReleases" at "http://artifactory.yandex.net/artifactory/yandex_spark_releases")
    .withAllowInsecureProtocol(true)

libraryDependencies ++= Seq(
    // зависимости от Spark
    "org.apache.spark" %% "spark-core" % "3.0.1" % Provided,
    "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided,
    // библиотека SPYT
    "ru.yandex" %% "spark-yt-data-source" % "1.50.0" % Provided
)
