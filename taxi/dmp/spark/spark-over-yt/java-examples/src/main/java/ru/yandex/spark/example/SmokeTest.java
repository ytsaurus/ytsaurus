package ru.yandex.spark.example;

import org.apache.spark.sql.SparkSession;

import ru.yandex.spark.yt.SparkAppJava;
import ru.yandex.yt.ytclient.proxy.CompoundClient;

public class SmokeTest extends SparkAppJava {
    @Override
    protected void doRun(String[] args, SparkSession spark, CompoundClient yt) {
        spark.read().format("yt").load("/sys/spark/examples/test_data").show();
    }

    public static void main(String[] args) {
        new SmokeTest().run(args);
    }
}
