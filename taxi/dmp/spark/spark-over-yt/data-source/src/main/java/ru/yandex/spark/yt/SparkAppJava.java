package ru.yandex.spark.yt;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import ru.yandex.spark.yt.fs.YtClientConfigurationConverter;
import ru.yandex.spark.yt.fs.YtClientProvider;
import ru.yandex.yt.ytclient.proxy.YtClient;


public abstract class SparkAppJava {
    public void run(String[] args) {
        SparkConf sparkConf = getSparkConf();
        YtClient yt = YtClientProvider.ytClient(YtClientConfigurationConverter.ytClientConfiguration(sparkConf));
        try {
            SparkSession spark = SessionUtils.buildSparkSession(sparkConf);
            try {
                doRun(args, spark, yt);
            } finally {
                spark.stop();
            }
        } finally {
            YtClientProvider.close();
        }
    }

    protected abstract void doRun(String[] args, SparkSession spark, YtClient yt);

    protected SparkConf getSparkConf() {
        return SessionUtils.prepareSparkConf();
    }
}
