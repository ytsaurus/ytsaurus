package ru.yandex.spark.yt.test

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, TestSuite}
import ru.yandex.spark.yt.wrapper.YtWrapper

trait TmpDir extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: TestSuite with LocalYt =>

  def testDir: String = s"//tmp/test-${self.getClass.getCanonicalName}"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"
  val hadoopTmpPath = s"ytTable:${tmpPath.drop(1)}"


  override def beforeAll(): Unit = {
    super.beforeAll()
    YtWrapper.removeDirIfExists(testDir, recursive = true)
    YtWrapper.createDir(testDir)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    YtWrapper.removeIfExists(tmpPath)
  }

  override def afterEach(): Unit = {
    YtWrapper.removeIfExists(tmpPath)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    YtWrapper.removeDirIfExists(testDir, recursive = true)
    super.afterAll()
  }
}
