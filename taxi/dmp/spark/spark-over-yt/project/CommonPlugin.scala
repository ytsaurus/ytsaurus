import Dependencies._
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.ShadeRule
import spyt.SparkForkVersion.sparkForkVersion
import spyt.SpytPlugin.autoImport._
import spyt.YtPublishPlugin

object CommonPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin

  object autoImport {
    val commonShadeRules: Seq[ShadeRule] = {
      // TODO get names from arrow dependency
      // Preserve arrow classes from shading
      val arrowBuffers = Seq("ArrowBuf", "ExpandableByteBuf", "LargeBuffer", "MutableWrappedByteBuf",
        "NettyArrowBuf", "PooledByteBufAllocatorL", "UnsafeDirectLittleEndian")
      val arrowRules = arrowBuffers.map(s"io.netty.buffer." + _).map(n => ShadeRule.rename(n -> n).inAll)
      arrowRules ++ Seq(
        ShadeRule.rename("javax.annotation.**" -> "shaded_spyt.javax.annotation.@1")
          .inLibrary("com.google.code.findbugs" % "annotations" % "2.0.3"),
        ShadeRule.zap("META-INF.org.apache.logging.log4j.core.config.plugins.Log4j2Plugins.dat")
          .inLibrary("org.apache.logging.log4j" % "log4j-core" % "2.11.0"),
        ShadeRule.rename("com.google.common.**" -> "shaded_spyt.com.google.common.@1")
          .inAll,
        ShadeRule.rename("io.netty.**" -> "shaded_spyt.io.netty.@1")
          .inAll
      )
    }

    val clusterShadeRules: Seq[ShadeRule] = commonShadeRules ++ Seq(ShadeRule.rename(
      "ru.yandex.spark.yt.submit.*" -> "ru.yandex.spark.yt.submit.@1",
      "ru.yandex.spark.yt.fs.YtFileSystem" -> "ru.yandex.spark.yt.fs.YtFileSystem",
      "ru.yandex.spark.yt.fs.eventlog.YtEventLogFileSystem" -> "ru.yandex.spark.yt.fs.eventlog.YtEventLogFileSystem",
      "ru.yandex.misc.log.**" -> "ru.yandex.misc.log.@1",
      "ru.yandex.**" -> "shadedyandex.ru.yandex.@1",
      "org.asynchttpclient.**" -> "shadedyandex.org.asynchttpclient.@1",
      "org.objenesis.**" -> "shadedyandex.org.objenesis.@1",
      "com.google.protobuf.**" -> "shadedyandex.com.google.protobuf.@1",
      "NYT.**" -> "shadedyandex.NYT.@1"
    ).inAll)

    val clientShadeRules: Seq[ShadeRule] = commonShadeRules ++ Seq(ShadeRule.rename(
      "ru.yandex.spark.yt.format.GlobalTransactionSparkListener" -> "ru.yandex.spark.yt.format.GlobalTransactionSparkListener",
      "ru.yandex.spark.yt.wrapper.**" -> "shadeddatasource.ru.yandex.spark.yt.wrapper.@1",
      "ru.yandex.yt.**" -> "shadeddatasource.ru.yandex.yt.@1",
      "ru.yandex.inside.**" -> "shadeddatasource.ru.yandex.inside.@1",
      "org.objenesis.**" -> "shadeddatasource.org.objenesis.@1",
      "com.google.protobuf.**" -> "shadeddatasource.com.google.protobuf.@1",
      "NYT.**" -> "shadeddatasource.NYT.@1"
    ).inAll)

    lazy val printTestClasspath = taskKey[Unit]("")
    lazy val commonDependencies = settingKey[Seq[ModuleID]]("")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.mavenCentral,
    resolvers += ("YandexMediaReleases" at "http://artifactory.yandex.net/artifactory/yandex_media_releases")
      .withAllowInsecureProtocol(true),
    resolvers += ("YandexSparkReleases" at "http://artifactory.yandex.net/artifactory/yandex_spark_releases")
      .withAllowInsecureProtocol(true),
    ThisBuild / version := (ThisBuild / spytClusterVersion).value,
    organization := "ru.yandex",
    name := s"spark-yt-${name.value}",
    scalaVersion := "2.12.8",
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    assembly / assemblyMergeStrategy := {
      case x if x endsWith "ahc-default.properties" => MergeStrategy.first
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x if x endsWith "Log4j2Plugins.dat" => MergeStrategy.last
      case x if x endsWith "git.properties" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_kqueue_x86_64.jnilib" => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
    assembly / test := {},
    publishTo := {
      val nexus = "http://artifactory.yandex.net/artifactory/"
      if (isSnapshot.value)
        Some(("snapshots" at nexus + "yandex_spark_snapshots").withAllowInsecureProtocol(true))
      else
        Some(("releases" at nexus + "yandex_spark_releases").withAllowInsecureProtocol(true))
    },
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    libraryDependencies ++= testDeps,
    Test / fork := true,
    printTestClasspath := {
      (Test / dependencyClasspath).value.files.foreach(f => println(f.getAbsolutePath))
    },
    ThisBuild / spytSparkForkDependency := {
      Seq(
        "org.apache.spark" %% "spark-core",
        "org.apache.spark" %% "spark-sql"
      ).map(_ % sparkForkVersion).map(_ excludeAll
        ExclusionRule(organization = "org.apache.httpcomponents")
      ).map(_ % Provided)
    },
    commonDependencies := yandexIceberg ++ (ThisBuild / spytSparkForkDependency).value ++ circe ++ logging.map(_ % Provided),
    Global / excludeLintKeys += commonDependencies
  )
}
