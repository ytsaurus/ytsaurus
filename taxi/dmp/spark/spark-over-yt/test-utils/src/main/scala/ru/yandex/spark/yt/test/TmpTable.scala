package ru.yandex.spark.yt.test

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import ru.yandex.spark.yt.utils.YtTableUtils

trait TmpTable extends Suite with BeforeAndAfterEach with BeforeAndAfterAll with LocalSpark {
  val testDir = "//tmp/test"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"


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
