import sbt._

object Dependencies {
  lazy val circeVersion = "0.12.3"
  lazy val circeYamlVersion = "0.12.0"
  lazy val scalatestVersion = "3.1.0"
  lazy val livyVersion = "0.8.0-incubating"
  lazy val sparkVersion = "3.2.2"
  lazy val ytsaurusClientVersion = "1.2.1"
  lazy val scalatraVersion = "2.7.0"
  lazy val mockitoVersion = "1.14.4"
  lazy val arrowVersion = "0.17.1"
  lazy val nettyVersion = "4.1.68.Final"

  lazy val circe = ("io.circe" %% "circe-yaml" % circeYamlVersion) +: Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)

  lazy val mockito = Seq(
    "org.mockito" %% "mockito-scala-scalatest" % mockitoVersion % Test,
    "org.mockito" %% "mockito-scala" % mockitoVersion % Test
  )

  lazy val dockerTest = Seq(
    "com.whisk" %% "docker-testkit-scalatest" % "0.9.9" % Test,
    "com.whisk" %% "docker-testkit-impl-docker-java" % "0.9.9" % Test,
    "com.kohlschutter.junixsocket" % "junixsocket-common" % "2.6.1" % Test,
    "com.kohlschutter.junixsocket" % "junixsocket-native-common" % "2.6.1" % Test
  ).map(_ excludeAll(
    ExclusionRule(organization = "io.netty")
  ))

  lazy val testDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
    "org.scalactic" %% "scalactic" % scalatestVersion % Test,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.0.0" % Test
  ) ++ mockito ++ dockerTest

  lazy val itTestDeps = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "it,test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "it,test"
  )

  lazy val scalatraTestDeps = Seq(
    "org.scalatra" %% "scalatra-scalatest" % scalatraVersion % "it" excludeAll
      ExclusionRule(organization = "org.scalatest")
  )

  lazy val spark = Seq("spark-core", "spark-sql").map { module =>
    "org.apache.spark" %% module % sparkVersion % Provided
  }

  lazy val sparkTest = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests"
  )

  lazy val ytsaurusClient = Seq(
    "tech.ytsaurus" % "ytsaurus-client" % ytsaurusClientVersion excludeAll(
    ExclusionRule(organization = "io.netty"),
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "org.apache.commons"),
    ExclusionRule(organization = "com.google.code.findbugs", name = "jsr305")
  ))

  lazy val scaldingArgs = Seq(
    "com.twitter" %% "scalding-args" % "0.17.4"
  )

  lazy val logging = Seq(
    "net.logstash.log4j" % "jsonevent-layout" % "1.7"
  )

  lazy val scalatra = Seq(
    "org.scalatra" %% "scalatra" % scalatraVersion,
    "org.eclipse.jetty" % "jetty-webapp" % "9.2.19.v20160908" % Compile,
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % Provided
  )

  lazy val sttp = Seq(
    "com.softwaremill.sttp.client" %% "core" % "2.1.4"
  )

  lazy val livy = Seq(
    "org.apache.livy" % "livy-assembly" % livyVersion % Provided excludeAll(
      ExclusionRule(organization = "org.json4s"),
      ExclusionRule(organization = "org.scala-lang.modules"),
      ExclusionRule(organization = "com.fasterxml.jackson.module"),
    )
  )
}
