import sbt.{Def, _}
import sbt.Keys._

object YtPublishPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = CommonPlugin

  object autoImport {
    val publishYt = taskKey[Unit]("Publish to yt directory")
    val publishYtArtifacts = taskKey[Seq[File]]("Yt publish artifacts")
  }

  import autoImport._
  import CommonPlugin.autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishYtArtifacts := Seq(),
    publishYt := {
      import scala.sys.process._
      import scala.language.postfixOps

      publishYtArtifacts.value.foreach{art =>
        s"cat ${art.getAbsolutePath}" #| s"yt write-file ${publishYtTo.value}/${art.getName}" !
      }
    }
  )
}
