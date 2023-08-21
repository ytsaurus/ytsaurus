package tech.ytsaurus.spyt;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter;
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider;
import tech.ytsaurus.client.CompoundClient;


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
