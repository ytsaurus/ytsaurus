package ru.yandex.yt.ytclient.rpc;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.YtCluster;


public class YtClientMultipleClustersTest {
    private YtCluster goodCluster, badCluster;
    private BusConnector connector;
    private final String user = "root";
    private final String token = "";

    @Before
    public void setup() throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("yt_proxy_port.txt"));
        final int proxyPort = Integer.valueOf(br.readLine());

        connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        final String host = "localhost";

        this.goodCluster = new YtCluster("local", host, proxyPort);
        this.badCluster = new YtCluster("bad", "bad.host", 13);
    }

    @Test
    public void testMultipleClusters() {
        YtClient goodAndBadClustersClient = new YtClient(
                connector,
                Cf.list(badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, goodCluster),
                "local",
                new RpcCredentials(user, token),
                new RpcOptions()
        );

        CompletableFuture<Void> goodAndBadWaitFuture = goodAndBadClustersClient.waitProxies();
        goodAndBadWaitFuture.join();
        if (goodAndBadWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided with at least one good cluster");
        }

        YtClient goodClustersClient = new YtClient(
                connector,
                Cf.list(goodCluster, goodCluster),
                "local",
                new RpcCredentials(user, token),
                new RpcOptions()
        );

        CompletableFuture<Void> goodWaitFuture = goodClustersClient.waitProxies();
        goodWaitFuture.join();
        if (goodWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided at least one good cluster");
        }

        YtClient badClustersClient = new YtClient(
                connector,
                Cf.list(badCluster, badCluster),
                "local",
                new RpcCredentials(user, token),
                new RpcOptions()
        );

        CompletableFuture<Void> badWaitFuture = badClustersClient.waitProxies();
        try {
            badWaitFuture.join();
        } catch (CompletionException thisIsFine) {
        }
        if (!badWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should have failed since YtClient was not provided with at least one good cluster");
        }
    }
}
