package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import static tech.ytsaurus.testlib.FutureUtils.waitFuture;


public class YTsaurusClientMultipleClustersTest extends YTsaurusClientTestBase {
    private YTsaurusCluster goodCluster;
    private YTsaurusCluster badCluster;
    private BusConnector connector;

    @Before
    public void setup() {
        connector = new DefaultBusConnector(new NioEventLoopGroup(0));

        this.goodCluster = new YTsaurusCluster(getYTsaurusAddress());
        this.badCluster = new YTsaurusCluster("bad", "bad.host", 13);
    }

    @Test
    public void testMultipleClusters() {
        final YTsaurusClientAuth auth = YTsaurusClientAuth.builder()
                .setUser("root")
                .setToken("")
                .build();

        var goodAndBadClustersClient = YTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setClusters(List.of(badCluster, badCluster, badCluster, badCluster, badCluster, badCluster, badCluster,
                        badCluster, badCluster, goodCluster))
                .setPreferredClusterName("local")
                .setAuth(auth)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(new RpcOptions())
                                .build()
                )
                .disableValidation()
                .build();

        CompletableFuture<Void> goodAndBadWaitFuture = goodAndBadClustersClient.waitProxies();
        goodAndBadWaitFuture.join();
        if (goodAndBadWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided with at least one good " +
                    "cluster");
        }

        var goodClustersClient = YTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setClusters(List.of(goodCluster, goodCluster))
                .setPreferredClusterName("local")
                .setAuth(auth)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(new RpcOptions())
                                .build()
                )
                .disableValidation()
                .build();

        CompletableFuture<Void> goodWaitFuture = goodClustersClient.waitProxies();
        goodWaitFuture.join();
        if (goodWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should not have failed since YtClient was provided at least one good cluster");
        }

        var badClustersClient = YTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setClusters(List.of(badCluster, badCluster))
                .setPreferredClusterName("local")
                .setAuth(auth)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(new RpcOptions())
                                .build()
                )
                .disableValidation()
                .build();

        CompletableFuture<Void> badWaitFuture = badClustersClient.waitProxies();
        waitFuture(badWaitFuture, 5000);
        if (!badWaitFuture.isCompletedExceptionally()) {
            Assert.fail("waitProxies() should have failed since YtClient was not provided with at least one good " +
                    "cluster");
        }
    }
}
