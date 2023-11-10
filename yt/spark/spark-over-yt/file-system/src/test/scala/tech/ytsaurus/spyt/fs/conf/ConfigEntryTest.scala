package tech.ytsaurus.spyt.fs.conf

import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigEntryTest extends AnyFlatSpec with Matchers {
  import ConfigEntry.implicits._
  import tech.ytsaurus.spyt.fs.conf._

  "ConfigEntry" should "retrieve configuration value" in {
    object SampleIntOption extends ConfigEntry[Int]("sample.int.option", Some(500))

    val sparkConf = new SparkConf
    sparkConf.set("spark.yt.sample.int.option", "200")

    sparkConf.ytConf(SampleIntOption) shouldBe 200
  }

  it should "return default value if the option is not set" in {
    object SampleLongOption extends ConfigEntry[Long]("sample.long.option", Some(2000L))

    val emptyConf = new SparkConf

    emptyConf.ytConf(SampleLongOption) shouldBe 2000L
  }

  it should "retrieve configuration value if some of the aliases is set" in {
    object OptionWithAliases extends ConfigEntry[Int]("base.option", Some(500), List("alias.option"))

    val sparkConf = new SparkConf
    sparkConf.set("spark.yt.alias.option", "200")

    sparkConf.ytConf(OptionWithAliases) shouldBe 200

  }

  it should "retrieve configuration value of the first alias in the list" in {
    object OptionWithAliases extends ConfigEntry[Int]("base.option", Some(500), List("alias.first", "alias.second"))

    val sparkConf = new SparkConf
    sparkConf.set("spark.yt.alias.first", "200")
    sparkConf.set("spark.yt.alias.second", "300")

    sparkConf.ytConf(OptionWithAliases) shouldBe 200
  }

}
