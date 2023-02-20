package tech.ytsaurus.client.rpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.YtCluster;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;

import ru.yandex.yt.testlib.LocalYt;

import static ru.yandex.yt.testlib.FutureUtils.waitFuture;


public class YtClientMultipleClustersTest {
    private YtCluster goodCluster;
    private YtCluster badCluster;
    private BusConnector connector;

    @Before
    public void setup() {
        connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        this.goodCluster = new YtCluster(LocalYt.getAddress());
        this.badCluster = new YtCluster("bad", "bad.host", 13);
    }

    @Test
    public void testMultipleClusters() {
        final YTsaurusClientAuth auth = YTsaurusClientAuth.builder()
                .setUser("root")
                .setToken("")
                .build();

        YtClient goodAndBadClustersClient = new YtClient(
                connector,
                List.of(badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, badCluster,
                        badCluster, badCluster, goodCluster),
                "local",
                auth,
                new RpcOptions()
        );

        CompletableFuture<Void> goodAndBadWaitFuture = goodAndBadClustersClient.waitProxies();
        goodAndBadWaitFuture.join();
        if (goodAndBadWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided with at least one good " +
                    "cluster");
        }

        YtClient goodClustersClient = new YtClient(
                connector,
                List.of(goodCluster, goodCluster),
                "local",
                auth,
                new RpcOptions()
        );

        CompletableFuture<Void> goodWaitFuture = goodClustersClient.waitProxies();
        goodWaitFuture.join();
        if (goodWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided at least one good cluster");
        }

        YtClient badClustersClient = new YtClient(
                connector,
                List.of(badCluster, badCluster),
                "local",
                auth,
                new RpcOptions()
        );

        CompletableFuture<Void> badWaitFuture = badClustersClient.waitProxies();
        waitFuture(badWaitFuture, 5000);
        if (!badWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should have failed since YtClient was not provided with at least one good " +
                    "cluster");
        }
    }
}
