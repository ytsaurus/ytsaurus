import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import sbtassembly.AssemblyPlugin.autoImport._
import ru.yandex.sbt.YtPublishPlugin
import Dependencies._

object CommonPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    resolvers += "YandexMediaReleases" at "http://artifactory.yandex.net/artifactory/yandex_media_releases",
    resolvers += "YandexSparkReleases" at "http://artifactory.yandex.net/artifactory/yandex_spark_releases",
    resolvers += MavenCache("local-maven", Path.userHome / ".m2" / "repository"),
    version in ThisBuild := "0.2.2-SNAPSHOT",
    organization := "ru.yandex",
    name := s"spark-yt-${name.value}",
    scalaVersion := "2.12.8",
    assemblyMergeStrategy in assembly := {
      case x if x endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x if x endsWith "Log4j2Plugins.dat" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("javax.annotation.**" -> "shaded.javax.annotation.@1")
        .inLibrary("com.google.code.findbugs" % "annotations" % "2.0.3"),
      ShadeRule.zap("META-INF.org.apache.logging.log4j.core.config.plugins.Log4j2Plugins.dat")
        .inLibrary("org.apache.logging.log4j" % "log4j-core" % "2.11.0")
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    publishTo := {
      val nexus = "http://artifactory.yandex.net/artifactory/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "yandex_spark_snapshots")
      else
        Some("releases" at nexus + "yandex_spark_releases")
    },
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    libraryDependencies ++= testDeps
  )
}
