import Dependencies._
import com.jsuereth.sbtpgp.PgpKeys.pgpPassphrase
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import spyt.SpytPlugin.autoImport._
import spyt.YtPublishPlugin

import java.nio.file.Paths

object CommonPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin

  object autoImport {
    lazy val printTestClasspath = taskKey[Unit]("")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false),
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.mavenCentral,
    resolvers += ("YTsaurusSparkReleases" at "https://repo1.maven.org/maven2"),
    resolvers += ("YTsaurusSparkSnapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"),
    ThisBuild / version := (ThisBuild / spytVersion).value,
    organization := "tech.ytsaurus",
    name := s"spark-yt-${name.value}",
    organizationName := "YTsaurus",
    organizationHomepage := Some(url("https://ytsaurus.tech/")),
    scmInfo := Some(ScmInfo(url("https://github.com/ytsaurus/ytsaurus"), "scm:git@github.com:ytsaurus/ytsaurus.git")),
    developers := List(
      Developer("Alexvsalexvsalex", "Alexey Shishkin", "alex-shishkin@ytsaurus.tech", url("https://ytsaurus.tech/")),
      Developer("alextokarew", "Aleksandr Tokarev", "atokarew@ytsaurus.tech", url("https://ytsaurus.tech/")),
    ),
    description := "Spark over YTsaurus",
    licenses := List(
      "The Apache License, Version 2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
    ),
    homepage := Some(url("https://ytsaurus.tech/")),
    pomIncludeRepository := { _ => false },
    scalaVersion := "2.12.15",
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    publishTo := {
      if (isSnapshot.value)
        Some("snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/")
      else
        Some("releases" at "https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
    },
    publishMavenStyle := true,
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    credentials += Credentials(Path.userHome / ".sbt" / ".ossrh_credentials"),
    libraryDependencies ++= testDeps,
    Test / fork := true,
    printTestClasspath := {
      (Test / dependencyClasspath).value.files.foreach(f => println(f.getAbsolutePath))
    },
    Global / pgpPassphrase := gpgPassphrase.map(_.toCharArray)
  )
}
