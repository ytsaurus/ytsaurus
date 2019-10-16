package ru.yandex.spark.yt

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait TmpTable extends Suite with BeforeAndAfterEach with BeforeAndAfterAll with LocalSpark {
  val testDir = "/home/sashbel/test"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    YtTableUtils.removeDirIfExists(testDir, recursive = true)
    YtTableUtils.createDir(testDir)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    YtTableUtils.removeTableIfExists(tmpPath)
  }

  override protected def afterEach(): Unit = {
    YtTableUtils.removeTableIfExists(tmpPath)
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    YtTableUtils.removeDirIfExists(testDir, recursive = true)
    super.afterAll()
  }
}
