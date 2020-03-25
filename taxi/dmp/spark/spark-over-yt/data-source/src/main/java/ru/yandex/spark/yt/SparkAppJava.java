package ru.yandex.spark.yt;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import ru.yandex.spark.yt.fs.YtClientConfigurationConverter;
import ru.yandex.spark.yt.fs.YtClientProvider;
import ru.yandex.yt.ytclient.proxy.YtClient;

public abstract class SparkAppJava {
    public void run(String[] args) {
        SparkConf sparkConf = getSparkConf();
        YtClient yt = YtClientProvider.ytClient(YtClientConfigurationConverter.apply(sparkConf));
        try {
            SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
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

    protected String getRemoteConfigPath() {
        return SessionUtils.remoteConfigPath();
    }

    protected SparkConf getSparkConf() {
        return SessionUtils.prepareSparkConf(getRemoteConfigPath());
    }
}
