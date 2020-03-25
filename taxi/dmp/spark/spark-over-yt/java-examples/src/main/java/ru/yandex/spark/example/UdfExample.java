package ru.yandex.spark.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import ru.yandex.spark.yt.SparkAppJava;
import ru.yandex.yt.ytclient.proxy.YtClient;

public class UdfExample extends SparkAppJava {
    public void doRun(String[] strings, SparkSession spark, YtClient yt) {
        Dataset<Row> df = spark.read().format("yt").load("/sys/spark/examples/example_1");
        UserDefinedFunction splitUdf = functions.udf((String s) -> s.split("-")[1], DataTypes.StringType);

        df
          .filter(df.col("id").gt(5))
          .select(splitUdf.apply(df.col("uuid")).as("value"))
          .write().mode(SaveMode.Overwrite).format("yt")
          .save("/sys/spark/examples/example_1_map");
    }

    public static void main(String[] args) {
        new UdfExample().run(args);
    }
}
