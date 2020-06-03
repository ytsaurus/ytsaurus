import com.typesafe.sbt.SbtNativePackager.autoImport.maintainer
import com.typesafe.sbt.packager.Keys.debianPackageMetadata
import com.typesafe.sbt.packager.debian.DebianPlugin.autoImport.{Debian, debianChangelog, debianSection}
import com.typesafe.sbt.packager.debian.{DebianPlugin, JDebPackaging, PackageMetaData}
import sbt.Keys._
import sbt._

import scala.sys.process.{ProcessBuilder => ScalaProcessBuilder, _}

object DebianPackagePlugin extends AutoPlugin {
  override def trigger = NoTrigger

  override def requires = super.requires && DebianPlugin && JDebPackaging

  object autoImport {
    val debPackageWithSources = taskKey[File]("Build debian package and .dsc file")
    val debPackageChanges = taskKey[File]("Build .changes file")
    val debPackageAddChangelog = taskKey[Unit]("Add message to changelog")
    val debPackageSign = taskKey[Unit]("Sign .dsc and .changes files by debsign")
    val debPackagePublish = taskKey[Unit]("Publish debian package to repository")
    val debPackageBuildAndPublish = taskKey[Unit]("Build, sign and publish debian package")

    val debPackageSourceControlFile = taskKey[File]("Build control file for dpkg-source")
    val debPackageFilesList = taskKey[File]("Create debian/files file")

    val debPackagePrefixPath = settingKey[String]("Base directory to add files")
    val debPackageSourceFormat = settingKey[String]("Required format of .dsc file")
    val debPackageVersion = settingKey[String]("Debian package version")
    val debPackageSignKey = settingKey[String]("Sign key for debsign")
    val debPackagePublishRepo = settingKey[String]("Debian package repository name")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    maintainer := "Alexandra Belousova <sashbel@yandex-team.ru>",
    debianSection := "misc",
    debianChangelog := Some(sourceDirectory.value / "debian" / "changelog"),

    debPackageSourceFormat := "1.0",
    debPackageSignKey := "40B21D83",
    debPackagePublishRepo := "common",

    debPackageWithSources := {
      val targetPath = (target in Debian).value
      val debianPath = targetPath / "debian"
      if (!debianPath.exists()) {
        IO.move(targetPath / "DEBIAN", debianPath)
      }
      val changelog = debianChangelog.value.get
      IO.copyFile(changelog, debianPath / "changelog")

      val format = debianPath / "source" / "format"
      IO.write(format, debPackageSourceFormat.value, java.nio.charset.Charset.defaultCharset)

      val control = debPackageSourceControlFile.value
      runProcess(
        Process(s"dpkg-source -c${control.getAbsolutePath} -b .", targetPath),
        "Failed to run dpkg-source"
      )

      targetPath / s"${targetPath.getName}.dsc"
    },
    debPackageWithSources := (debPackageWithSources dependsOn (packageBin in Debian)).value,
    debPackageFilesList := {
      val debName = (name in Debian).value + "_" + (version in Debian).value
      val debianPath = (target in Debian).value / "debian"
      val output = debianPath / "files"
      val content = s"""${debName}_all.deb misc optional
                       |$debName.dsc text important""".stripMargin

      IO.write(output, content)
      output
    },
    debPackageChanges := {
      val debName = (name in Debian).value + "_" + (version in Debian).value
      val control = debPackageSourceControlFile.value
      val files = debPackageFilesList.value
      val output = target.value / s"${debName}_all.changes"
      runProcess(
        Process(
          s"dpkg-genchanges -f${files.getAbsolutePath} -c${control.getAbsolutePath} -O${output.getAbsolutePath}",
          (target in Debian).value
        ),
        "Failed to run dpkg-genchanges"
      )
      output
    },
    debPackageAddChangelog := {
      import scala.language.postfixOps
      import scala.sys.process._

      val isStable = Option(System.getProperty("stable")).exists(_.toBoolean)
      val distribution = if (isStable) "stable" else "unstable"
      val message = System.getProperty("message")

      runProcess(
        Process(s"""dch -v ${debPackageVersion.value} --distribution $distribution "$message"""", sourceDirectory.value),
        "Failed to run dch"
      )
    },
    debPackageSourceControlFile := {
      val data = (debianPackageMetadata in Debian).value
      if (data.info.description == null || data.info.description.isEmpty) {
        sys.error(
          """packageDescription in Debian cannot be empty. Use
                 packageDescription in Debian := "My package Description"""")
      }
      val cfile = (target in Debian).value / "debian" / "control-source"
      IO.write(cfile, makeControlFileContent(data), java.nio.charset.Charset.defaultCharset)
      cfile
    },
    debPackageSign := {
      val debName = (name in Debian).value + "_" + (version in Debian).value

      signFile(debPackageSignKey.value, target.value / s"$debName.dsc")
      signFile(debPackageSignKey.value, target.value / s"${debName}_all.changes")
    },
    debPackagePublish := {
      val debName = (name in Debian).value + "_" + (version in Debian).value
      val changes = target.value / s"${debName}_all.changes"

      runProcess(
        s"dupload --to ${debPackagePublishRepo.value} ${changes.getAbsolutePath}",
        "Failed to run dupload"
      )
    },
    debPackageBuildAndPublish := Def.sequential(
      debPackageAddChangelog,
      debPackageWithSources,
      debPackageChanges,
      debPackageSign,
      debPackagePublish
    ).value
  )

  private def runProcess(process: ScalaProcessBuilder, failMessage: String): Unit = {
    val exit = process !

    if (exit != 0) {
      throw new RuntimeException(failMessage)
    }
  }

  private def makeControlFileContent(data: PackageMetaData): String = {
    s"""Source: ${data.info.name}
       |Section: ${data.section}
       |Priority: ${data.priority}
       |Maintainer: ${data.info.maintainer}
       |Standards-Version: 4.1.2
       |
       |Package: ${data.info.name}
       |Architecture: ${data.architecture}
       |Description: ${data.info.summary}
       | ${data.info.description}
       |""".stripMargin
  }

  private def signFile(signKey: String, file: File): Unit = {
    runProcess(s"debsign -k $signKey ${file.getAbsolutePath}", "Failed to run debsign")
  }
}

