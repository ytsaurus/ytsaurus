package ru.yandex.spark.example;

import org.apache.spark.sql.SparkSession;

public class YtCloseTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.read().format("yt").load("/sys/spark/examples/example_1").show();
    }
}
