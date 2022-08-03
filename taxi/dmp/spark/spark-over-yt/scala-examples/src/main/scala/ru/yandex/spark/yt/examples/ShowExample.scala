package ru.yandex.spark.yt.examples

import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt._

object ShowExample extends App {
    val spark = SparkSession.builder.getOrCreate()
    spark.read.yt("//home/spark/examples/test_data").show()
}
