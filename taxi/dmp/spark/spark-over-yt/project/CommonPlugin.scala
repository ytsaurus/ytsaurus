import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import sbtassembly.AssemblyPlugin.autoImport._
import Dependencies._

object CommonPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.mavenCentral,
    resolvers += "YandexMediaReleases" at "http://artifactory.yandex.net/artifactory/yandex_media_releases",
    resolvers += "YandexSparkReleases" at "http://artifactory.yandex.net/artifactory/yandex_spark_releases",
    version in ThisBuild := "1.6.2-SNAPSHOT",
    organization := "ru.yandex",
    name := s"spark-yt-${name.value}",
    scalaVersion := "2.12.8",
    assemblyMergeStrategy in assembly := {
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x if x endsWith "Log4j2Plugins.dat" => MergeStrategy.last
      case x if x endsWith "git.properties" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_epoll_x86_64.so" => MergeStrategy.last
      case x if x endsWith "libnetty_transport_native_kqueue_x86_64.jnilib" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly := {
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
        ShadeRule.rename("com.google.common.**" -> "shaded_spyt.com.google.common.@1")
          .inAll,
        ShadeRule.rename("io.netty.**" -> "shaded_spyt.io.netty.@1")
          .inAll
      )
    },
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    publishTo := {
      val nexus = "http://artifactory.yandex.net/artifactory/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "yandex_spark_snapshots")
      else
        Some("releases" at nexus + "yandex_spark_releases")
    },
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    libraryDependencies ++= testDeps,
    fork in Test := true
  )
}
