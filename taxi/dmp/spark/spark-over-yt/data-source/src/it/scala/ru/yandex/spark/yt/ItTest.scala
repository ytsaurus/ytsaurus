package ru.yandex.spark.yt

import io.circe.parser._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ru.yandex.spark.yt.ProgrammingLanguage._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider

import java.io.File
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.sys.process._

class ItTest extends FlatSpec with Matchers with BeforeAndAfterAll with ItTestUtils with HumeYtClient {
  val confName = "spark.hadoop.yt.byop.enabled"

  case class Conf(publish: Boolean = false,
                  rebuildSpark: Boolean = false,
                  startClusters: Boolean = false,
                  publishTestJob: Boolean = false)

  private implicit def anyToOption[T](x: T): Option[T] = Some(x)

  override def oldVersion: String = "0.2.1"

  override def newVersion: String = "0.3.0-SNAPSHOT"

  private def beforeByop(version: String): Boolean = version < "0.2.0"

  def testDisabled(cluster: String)
                  (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = oldVersion,
      expected = Map(confName -> Some("false").filterNot(_ => beforeByop(oldVersion)))
    )
    testDisabledNewSpyt(cluster)
  }

  def testEnabled(cluster: String)
                 (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = oldVersion,
      expected = Map(confName -> Some("false").filterNot(_ => beforeByop(oldVersion)))
    )
    testEnabledNewSpyt(cluster)
  }

  def testLanguage(language: ProgrammingLanguage): Unit = {
    implicit val l: ProgrammingLanguage = language
    implicit val ec: ExecutionContext = ExecutionContext.global
    val newDisable = Future(testDisabled("new_disable"))
    val newEnable = Future(testEnabled("new_enable"))
    val oldNewLauncher = Future(if (beforeByop(oldVersion)) testDisabled("old_new_launcher") else testEnabled("old_new_launcher"))
    val oldOldLauncher = Future(testDisabled("old_old_launcher"))
    Await.result(newDisable.zip(newEnable).zip(oldNewLauncher).zip(oldOldLauncher), 20 minutes)
  }

  def testEnabledNewSpyt(cluster: String)
                        (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = newVersion, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = false, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = true, expected = Map(confName -> "true"))
  }

  def testEnabledOldSpyt(cluster: String)
                        (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = oldVersion, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = oldVersion, enableByop = false, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = oldVersion, enableByop = true, expected = Map(confName -> "true"))
  }

  def testDisabledNewSpyt(cluster: String)
                         (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = newVersion, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = false, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = true, expected = Map(confName -> "false"))
  }

  def testDisabledOldSpyt(cluster: String)
                         (implicit l: ProgrammingLanguage): Unit = {
    submitAndCheckConf(cluster, spytVersion = newVersion, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = false, expected = Map(confName -> "false"))
    submitAndCheckConf(cluster, spytVersion = newVersion, enableByop = true, expected = Map(confName -> "false"))
  }


  def testNewLocalPython(language: LocalPython): Unit = {
    implicit val l: ProgrammingLanguage = language
    testDisabledNewSpyt("new_disable")
    testEnabledNewSpyt("new_enable")
    if (beforeByop(oldVersion)) testDisabledNewSpyt("old_new_launcher") else testEnabledNewSpyt("old_new_launcher")
    testDisabledNewSpyt("old_old_launcher")
  }

  def testOldLocalPython(language: LocalPython): Unit = {
    implicit val l: ProgrammingLanguage = language
    testDisabledOldSpyt("new_disable")
    testEnabledOldSpyt("new_enable")
    testEnabledOldSpyt("old_new_launcher")
    testDisabledOldSpyt("old_old_launcher")
    //    submitAndCheckConf("new_disable", expected = Map(confName -> None))
    //    submitAndCheckConf("new_enable", expected = Map(confName -> None))
    //    submitAndCheckConf("old_new_launcher", expected = Map(confName -> None))
    //    submitAndCheckConf("old_old_launcher", expected = Map(confName -> None))
  }

  "ItTest" should "work for scala" in {
    testLanguage(Scala)
  }

  it should "work for python" in {
    testLanguage(Python2)
  }

  it should "work for local python2, new yandex-spyt" in {
    testNewLocalPython(LocalPython2(newVersion))
  }

  it should "work for local python3, new yandex-spyt" in {
    testNewLocalPython(LocalPython3(newVersion))
  }

  it should "work for local python2, old yandex-spyt" in {
    testOldLocalPython(LocalPython2(oldVersion))
  }

  it should "work for local python3, old yandex-spyt" in {
    testOldLocalPython(LocalPython3(oldVersion))
  }

  it should "work for old spark-submit" in {
    val oldPython = LocalPython2(oldVersion)
    val cluster = "old_old_launcher"

    submitAndCheckConf(cluster, expected = Map(confName -> None), spytVersion = oldVersion, localPython = oldPython)(Python2)
    submitAndCheckConf(cluster, expected = Map(confName -> None), spytVersion = oldVersion, localPython = oldPython)(Scala)
    submitAndCheckConf(cluster, expected = Map(confName -> None))(oldPython)
  }

  it should "start clusters" ignore {
    implicit val language = Scala

    startCluster("0.2.0~beta1", "new_enable")
    submitAndCheckConf("new_enable", spytVersion = newVersion, expected = Map(confName -> "true"))

    startCluster("0.2.0~beta1", "new_disable", Seq("--disable-byop"))
    submitAndCheckConf("new_disable", spytVersion = newVersion, expected = Map(confName -> "false"))

    startCluster(oldVersion, "old_enable", localPython = LocalPython3(newVersion))
    submitAndCheckConf("old_enable", spytVersion = newVersion, expected = Map(confName -> "false"))

    startCluster(oldVersion, "old_disable", Seq("--disable-byop"))
    submitAndCheckConf("old_disable", spytVersion = newVersion, expected = Map(confName -> "false"))

    startCluster(oldVersion, "old_old", localPython = LocalPython2(oldVersion))
    submitAndCheckConf("old_old", spytVersion = newVersion, expected = Map(confName -> "false"))

    startCluster(oldVersion, "old_old3", localPython = LocalPython3(oldVersion))
    submitAndCheckConf("old_old", spytVersion = newVersion, expected = Map(confName -> "false"))
  }

  def resource(name: String): String = {
    getClass.getResource(name).getPath
  }

  def submitAndCheckConf(cluster: String, enableByop: Option[Boolean] = None,
                         expected: Map[String, Option[String]] = Map(),
                         spytVersion: Option[String] = None,
                         localPython: LocalPython = LocalPython2(newVersion))
                        (implicit l: ProgrammingLanguage): Unit = {
    val outputPath = s"//home/sashbel/test-conf/test-conf-${UUID.randomUUID()}"
    YtWrapper.removeIfExists(outputPath)

    l match {
      case Scala => submitScala(cluster, outputPath, spytVersion.get, enableByop, localPython)
      case Python2 => submitPython(cluster, outputPath, spytVersion.get, enableByop, localPython)
      case pl: LocalPython => runLocalPython(pl, cluster, outputPath, enableByop)
    }

    val confString = YtWrapper.readFileAsString(outputPath)
    val conf = decode[Map[String, String]](confString) match {
      case Right(value) => value
      case Left(error) => throw error
    }
    YtWrapper.remove(outputPath)

    expected.foreach {
      case (key, Some(value)) => conf(key).toLowerCase() shouldEqual value
      case (key, None) => conf.contains(key) shouldEqual false
    }
  }

  def runLocalPython(python: LocalPython,
                     cluster: String,
                     outputPath: String,
                     enableByop: Option[Boolean]): Unit = {
    val command = s"${python.pythonBin} " +
      s"${resource("test_conf.py")} " +
      s"--discovery-path ${discoveryPath(cluster)} " +
      s"--path $outputPath " +
      enableByop.map(b => s"--conf spark.hadoop.yt.byop.enabled=$b ").getOrElse("")

    println(command)
    val exitCode = Process(
      command,
      new File("."),
      "PYTHONPATH" -> ""
    ).run().exitValue()

    exitCode shouldEqual 0
  }

  def submitScala(cluster: String,
                  outputPath: String,
                  spytVersion: String,
                  enableByop: Option[Boolean],
                  localPython: LocalPython): Unit = {
    val command = s"${localPython.sparkSubmitYt} " +
      "--proxy hume " +
      s"--discovery-path ${discoveryPath(cluster)} " +
      "--deploy-mode cluster " +
      enableByop.map(b => s"--conf spark.hadoop.yt.byop.enabled=$b ").getOrElse("") +
      s"--spyt-version $spytVersion " +
      "--class ru.yandex.spark.test.TestConf " +
      "yt:///home/sashbel/spark-yt-test-job-assembly-0.2.0-SNAPSHOT.jar " +
      s"--path $outputPath"

    println(command)

    val exitCode = Process(
      command,
      new File("."),
      "PYTHONPATH" -> "",
      "SPARK_HOME" -> localPython.sparkHome
    ).run().exitValue()

    exitCode shouldEqual 0
  }

  def submitPython(cluster: String,
                   outputPath: String,
                   spytVersion: String,
                   enableByop: Option[Boolean],
                   localPython: LocalPython): Unit = {
    val command = s"${localPython.sparkSubmitYt} " +
      "--proxy hume " +
      s"--discovery-path ${discoveryPath(cluster)} " +
      "--deploy-mode cluster " +
      enableByop.map(b => s"--conf spark.hadoop.yt.byop.enabled=$b ").getOrElse("") +
      s"--spyt-version $spytVersion " +
      "yt:///home/sashbel/test_conf.py " +
      s"--path $outputPath"

    println(command)
    val exitCode = Process(
      command,
      new File("."),
      "PYTHONPATH" -> "",
      "SPARK_HOME" -> localPython.sparkHome
    ).run().exitValue()

    exitCode shouldEqual 0
  }


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = Conf()
    if (conf.publish) {
      val publishScript = if (conf.rebuildSpark) "publish_all.sh" else "publish_no_spark_build.sh"
      resource(publishScript) !
    }
    val disableByop = Seq("--disable-byop")
    val enableByop = Seq("--enable-byop")
    if (conf.startClusters) {
      Seq(
        () => startCluster("0.3.0~beta1", "new_enable", args = enableByop),
        () => startCluster("0.3.0~beta1", "new_disable", args = disableByop),
        () => startCluster(oldVersion, "old_new_launcher", args = enableByop),
        () => startCluster(oldVersion, "old_old_launcher", localPython = LocalPython2(oldVersion))
      ).par.map(_.apply()).toList
    }
    if (conf.publishTestJob) {
      resource("publish_test_job.sh") !
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    YtClientProvider.close()
  }
}
