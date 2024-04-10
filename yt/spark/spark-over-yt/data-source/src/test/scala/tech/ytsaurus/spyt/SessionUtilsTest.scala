package tech.ytsaurus.spyt

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.SessionUtils.mergeConfs

class SessionUtilsTest extends FlatSpec with Matchers {

  private val someConf = "spark.hadoop.yt.conf.enabled"
  private val someConf2 = "spark.hadoop.yt.conf2.enabled"

  it should "block user conf by disabled enabler" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    val result = mergeConfs(conf, Map.empty, Map.empty, Map.empty, Map(someConf -> "false", someConf2 -> "false"))
    result.get(someConf).toBoolean shouldBe false
    result.getOption(someConf2) shouldBe None
  }

  it should "pass user conf when enabler is enabled" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    conf.set(someConf2, "true")
    val result = mergeConfs(conf, Map.empty, Map.empty, Map.empty, Map(someConf -> "true"))
    result.get(someConf).toBoolean shouldBe true
    result.get(someConf2).toBoolean shouldBe true
  }

  it should "block cluster confs by disabled enabler" in {
    val conf = new SparkConf(false)
    val globalConf = Map(someConf -> "true")
    val clusterConf = Map(someConf2 -> "true")
    val result = mergeConfs(conf, globalConf, Map.empty, clusterConf, Map(someConf -> "false", someConf2 -> "false"))
    result.get(someConf).toBoolean shouldBe false
    result.get(someConf2).toBoolean shouldBe false
  }

  it should "prioritize user defined config" in {
    val conf = new SparkConf(false)
    conf.set(someConf, "true")
    conf.set(someConf2, "true")
    val globalConf = Map(someConf -> "false")
    val clusterConf = Map(someConf2 -> "false")
    val result = mergeConfs(conf, globalConf, Map.empty, clusterConf, Map(someConf -> "true", someConf2 -> "false"))
    result.get(someConf).toBoolean shouldBe true
    result.get(someConf2).toBoolean shouldBe false
  }
}
