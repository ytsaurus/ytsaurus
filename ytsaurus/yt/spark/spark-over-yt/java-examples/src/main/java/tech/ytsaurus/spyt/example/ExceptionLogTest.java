package tech.ytsaurus.spyt.example;

import org.apache.spark.sql.SparkSession;

import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.spyt.SparkAppJava;

public class ExceptionLogTest extends SparkAppJava {
    @Override
    protected void doRun(String[] args, SparkSession spark, CompoundClient yt) {
        int a = 1 / 0;
        spark.read()
                .format("yt")
                .load("/sys/spark/examples/test_data")
                .show();
    }

    public static void main(String[] args) {
        new SmokeTest().run(args);
    }
}
