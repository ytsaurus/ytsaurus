import sbt.Keys.test
import sbt._
import sbt.plugins.JvmPlugin
import sbtbuildinfo.BuildInfoPlugin
import spyt.YtPublishPlugin

import java.time.Duration

object E2ETestPlugin extends AutoPlugin {
  override def trigger = AllRequirements

  override def requires = JvmPlugin && YtPublishPlugin && BuildInfoPlugin

  object autoImport {
    lazy val e2eDirTTL = Duration.ofMinutes(30).toMillis
    lazy val e2eTest = taskKey[Unit]("Run e2e tests")
  }

  import autoImport._
  import YtPublishPlugin.autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    e2eTest := Def.sequential(
      publishYt,
      Test / test
    ).value
  )

}
