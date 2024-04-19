package org.apache.spark

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * This test was written because SparkConfSpyt used @Subclass annotation which lead to changing signature of all
 * builder pattern methods, i.e. that were returning SparkConf instances. So other classes couldn't find these
 * methods by their original signature. So, @Subclass was changed to @Decarate and this test ensures that everything
 * works as expected.
 */
class SparkConfTest extends AnyFlatSpec with Matchers {
  behavior of "SparkConf"

  it should "set a missing attribute" in {
    val conf = new SparkConf()
    conf.getOption("some.key") shouldBe None

    conf.setIfMissing("some.key", "some.value")
    conf.getOption("some.key") shouldBe Some("some.value")

    conf.setIfMissing("some.key", "updated.value")
    conf.getOption("some.key") shouldBe Some("some.value")
  }

  it should "set spark properties from environment" in {
    //Setting environment variable value in java.util.Collections.UnmodifiableMap using reflection
    val envMap = System.getenv()
    val field = envMap.getClass.getDeclaredField("m")
    field.setAccessible(true)
    val innerEnvMap = field.get(envMap).asInstanceOf[java.util.Map[String, String]]
    innerEnvMap.put("SPARK_YT_VERSION", "value")
    field.setAccessible(false)

    val conf = new SparkConf()
    conf.getOption("spark.yt.version") shouldBe Some("value")
  }
}
