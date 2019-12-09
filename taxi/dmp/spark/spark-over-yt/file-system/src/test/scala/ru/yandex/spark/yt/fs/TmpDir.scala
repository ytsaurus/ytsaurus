package ru.yandex.spark.yt.fs

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import ru.yandex.spark.yt.utils.YtTableUtils
import ru.yandex.yt.ytclient.proxy.YtClient

trait TmpDir extends Suite with BeforeAndAfterEach with BeforeAndAfterAll {
  val testDir = "/tmp/test"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"

  def yt: YtClient

  implicit lazy val y = yt

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    YtTableUtils.removeDirIfExists(testDir, recursive = true)
    YtTableUtils.createDir(testDir)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    YtTableUtils.removeIfExists(tmpPath)
  }

  override protected def afterEach(): Unit = {
    YtTableUtils.removeIfExists(tmpPath)
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    YtTableUtils.removeDirIfExists(testDir, recursive = true)
    super.afterAll()
  }
}
