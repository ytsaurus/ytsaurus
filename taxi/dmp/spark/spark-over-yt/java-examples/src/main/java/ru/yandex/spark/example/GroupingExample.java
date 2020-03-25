package ru.yandex.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import ru.yandex.spark.yt.SparkAppJava;
import ru.yandex.yt.ytclient.proxy.YtClient;

public class GroupingExample extends SparkAppJava {
    public void doRun(String[] args, SparkSession spark, YtClient yt) {
        Dataset<Row> df = spark.read().format("yt").load("/sys/spark/examples/example_1");
        Dataset<Row> dictDf = spark.read().format("yt").load("/sys/spark/examples/example_dict");

        df
          .join(dictDf, df.col("uuid").equalTo(dictDf.col("uuid")), "left_outer")
          .groupBy("count")
          .agg(functions.max("id").as("max_id"))
          .repartition(1)
          .write().mode(SaveMode.Overwrite).format("yt").save("/sys/spark/examples/example_1_agg");
    }

    public static void main(String[] args) {
        new GroupingExample().run(args);
    }

    @Override
    protected String getRemoteConfigPath() {
        return "//sys/spark/conf/snapshots/spark-launch-conf";
    }

    @Override
    protected SparkConf getSparkConf() {
        return super.getSparkConf().setAppName("Custom name");
    }
}
