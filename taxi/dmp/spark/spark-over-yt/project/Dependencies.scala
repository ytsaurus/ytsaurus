import sbt._

object Dependencies {
  lazy val circeVersion = "0.11.1"
  lazy val scalatestVersion = "3.0.8"
  lazy val sparkVersion = "2.4.4"
  lazy val yandexIcebergVersion = "6453303"
  lazy val slf4jVersion = "1.7.28"

  lazy val circe = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

  lazy val testDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
    "org.scalactic" %% "scalactic" % scalatestVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )

  lazy val spark = Seq(
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql"
  ).map(_ % sparkVersion % Provided).excludeLogging

  lazy val yandexIceberg = Seq(
    "ru.yandex" % "iceberg-inside-yt" % yandexIcebergVersion excludeAll (
      ExclusionRule(organization = "com.fasterxml.jackson.core")
    )
  ).excludeLogging

  lazy val grpc = Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
  )

  lazy val scaldingArgs = Seq(
    "com.twitter" %% "scalding-args" % "0.17.4"
  )

  lazy val logging = Seq(
    "org.slf4j" % "slf4j-log4j12",
    "org.slf4j" % "slf4j-api",
    "org.slf4j" % "jul-to-slf4j"
  ).map(_ % slf4jVersion)

  implicit class RichDependencies(deps: Seq[ModuleID]) {
    def excludeLogging: Seq[ModuleID] = deps.map(_.excludeAll(ExclusionRule(organization = "org.slf4j")))
  }
}
