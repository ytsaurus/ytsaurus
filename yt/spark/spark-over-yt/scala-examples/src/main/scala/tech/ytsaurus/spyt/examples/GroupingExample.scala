package tech.ytsaurus.spyt.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

object GroupingExample extends App {
    val conf = new SparkConf().setAppName("Grouping example")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val df = spark.read.yt("//home/spark/examples/example_1")
    val dictDf = spark.read.yt("//home/spark/examples/example_dict")

    df
        .join(dictDf, Seq("uuid"), "left_outer")
        .groupBy("count")
        .agg(max("id") as "max_id")
        .repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .optimizeFor(OptimizeMode.Scan)
        .yt("//home/spark/examples/example_1_agg")
}
