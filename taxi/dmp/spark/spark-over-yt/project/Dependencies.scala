import sbt._

object Dependencies {
  lazy val circeVersion = "0.12.3"
  lazy val circeYamlVersion = "0.12.0"
  lazy val scalatestVersion = "3.0.8"
  lazy val sparkVersion = "2.4.4"
  lazy val yandexIcebergVersion = "6453303"
  lazy val slf4jVersion = "1.7.28"
  lazy val scalatraVersion = "2.7.0"

  lazy val circe = ("io.circe" %% "circe-yaml" % circeYamlVersion) +: Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

  lazy val testDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
    "org.scalactic" %% "scalactic" % scalatestVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  )

  lazy val itTestDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "it,test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "it,test"
  )

  lazy val scalatraTestDeps = Seq(
    "org.scalatra" %% "scalatra-scalatest" % scalatraVersion % "it" excludeAll
      ExclusionRule(organization = "org.scalatest")
  )

  lazy val spark = Seq(
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql"
  ).map(_ % sparkVersion % Provided).excludeLogging

  lazy val yandexIceberg = Seq(
    "ru.yandex" % "iceberg-inside-yt" % yandexIcebergVersion excludeAll (
      ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "ru.yandex", name = "java-ytclient"),
    ),
    "ru.yandex" % "java-ytclient" % "custom"
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

  lazy val scalatra = Seq(
    "org.scalatra" %% "scalatra" % scalatraVersion,
    "org.eclipse.jetty" % "jetty-webapp" % "9.2.19.v20160908" % Compile,
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % Provided
  )

  lazy val sttp = Seq(
    "com.softwaremill.sttp.client" %% "core" % "2.1.4"
  )

  implicit class RichDependencies(deps: Seq[ModuleID]) {
    def excludeLogging: Seq[ModuleID] = deps.map(_.excludeAll(ExclusionRule(organization = "org.slf4j")))
  }
}
