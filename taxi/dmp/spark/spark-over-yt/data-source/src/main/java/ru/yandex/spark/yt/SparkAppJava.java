package ru.yandex.spark.yt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.SparkSession;

import ru.yandex.spark.yt.fs.YtClientProvider;
import ru.yandex.yt.ytclient.proxy.YtClient;

public interface SparkAppJava {
    default void run(String[] args) {
        try {
            SparkConf sparkConf = new SparkConf();
            Configuration hadoopConf = SparkHadoopUtil.get().newConfiguration(sparkConf);
            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
            try {
                doRun(args, spark, YtClientProvider.ytClient());
            } finally {
                spark.stop();
            }
        } finally {
            YtClientProvider.close();
        }

    }

    void doRun(String[] args, SparkSession spark, YtClient yt);
}
