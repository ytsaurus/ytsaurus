package ru.yandex.spark.yt.test

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, TestSuite}
import ru.yandex.spark.yt.wrapper.YtWrapper

trait TmpDir extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: TestSuite with LocalYtClient =>

  def testDir: String = s"//tmp/test-${self.getClass.getCanonicalName}"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    YtWrapper.removeDirIfExists(testDir, recursive = true)
    YtWrapper.createDir(testDir)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    YtWrapper.removeIfExists(tmpPath)
  }

  override protected def afterEach(): Unit = {
    YtWrapper.removeIfExists(tmpPath)
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    YtWrapper.removeDirIfExists(testDir, recursive = true)
    super.afterAll()
  }
}
