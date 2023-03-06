package tech.ytsaurus.spyt.examples

import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt._

object ShowExample extends App {
    val spark = SparkSession.builder.getOrCreate()
    spark.read.yt("//home/spark/examples/test_data").show()
}
