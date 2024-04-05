
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.deploy.ytsaurus.Config.SPARK_PRIMARY_RESOURCE
import org.apache.spark.internal.config.{FILES, JARS, SUBMIT_PYTHON_FILES}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

class YTsaurusOperationManagerSuite extends SparkFunSuite with BeforeAndAfter with Matchers {

  test("Generate application files for python spark-submit in cluster mode") {
    val conf = new SparkConf()
    conf.set(SUBMIT_PYTHON_FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq("//path/to/my/super/app.py", "//path/to/my/super/lib.zip")
  }

  test("Generate application files for python spark-submit in client mode") {
    val conf = new SparkConf()
    conf.set(FILES, Seq("yt:/path/to/my/super/lib.zip"))
    conf.set(SUBMIT_PYTHON_FILES, Seq("/tmp/spark-164a106b-cc57-4bb6-b30f-e67b7bbb8d8a/lib.zip"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.py")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq("//path/to/my/super/app.py", "//path/to/my/super/lib.zip")
  }

  test("Generate application files for java spark-submit") {
    val conf = new SparkConf()
    conf.set(JARS, Seq("yt:/path/to/my/super/lib.jar", "yt:///path/to/my/super/app.jar"))
    conf.set(SPARK_PRIMARY_RESOURCE, "yt:///path/to/my/super/app.jar")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result should contain theSameElementsAs Seq("//path/to/my/super/lib.jar", "//path/to/my/super/app.jar")
  }

  test("Generate application files for spark-shell") {
    val conf = new SparkConf()
    conf.set(SPARK_PRIMARY_RESOURCE, "spark-shell")

    val result = YTsaurusOperationManager.applicationFiles(conf)

    result shouldBe empty
  }
}
