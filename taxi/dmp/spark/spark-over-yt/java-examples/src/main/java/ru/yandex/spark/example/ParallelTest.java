package ru.yandex.spark.example;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.SparkSession;

import ru.yandex.spark.yt.SparkAppJava;
import ru.yandex.yt.ytclient.proxy.YtClient;

public class ParallelTest extends SparkAppJava {
    private static ExecutorService pool = Executors.newFixedThreadPool(4);

    @Override
    protected void doRun(String[] strings, SparkSession sparkSession, YtClient ytClient) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            CompletableFuture.supplyAsync(() -> {
                Thread.currentThread().setContextClassLoader(loader);
                sparkSession.read().format("yt").load("/sys/spark/examples/example_1").show();
                return 1;
            }, pool).join();
        } finally {
            pool.shutdown();
        }
    }

    public static void main(String[] args) {
        new ParallelTest().run(args);
    }
}
