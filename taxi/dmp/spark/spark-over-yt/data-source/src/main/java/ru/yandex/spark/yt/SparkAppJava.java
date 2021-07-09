package ru.yandex.spark.yt;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter;
import ru.yandex.spark.yt.wrapper.client.YtClientProvider;
import ru.yandex.yt.ytclient.proxy.CompoundClient;


public abstract class SparkAppJava {
    public void run(String[] args) {
        SparkConf sparkConf = getSparkConf();
        CompoundClient yt = YtClientProvider.ytClient(YtClientConfigurationConverter.ytClientConfiguration(sparkConf));
        SparkSession spark = SessionUtils.buildSparkSession(sparkConf);
        doRun(args, spark, yt);
    }

    protected abstract void doRun(String[] args, SparkSession spark, CompoundClient yt);

    protected SparkConf getSparkConf() {
        return SessionUtils.prepareSparkConf();
    }
}
