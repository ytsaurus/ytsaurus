package ru.yandex.spark.example;

import org.apache.spark.sql.SparkSession;

public class YtCloseExceptionTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.read().format("yt").load("/sys/spark/examples/example_1").show();
        throw new RuntimeException("This is fine");
    }
}
