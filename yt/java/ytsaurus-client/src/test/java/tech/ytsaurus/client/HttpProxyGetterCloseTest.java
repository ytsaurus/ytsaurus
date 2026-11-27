package tech.ytsaurus.client;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Random;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assume;
import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcOptions;

import static org.junit.Assert.assertTrue;

public class HttpProxyGetterCloseTest {
    @Test
    public void testCloseTerminatesSelectorManagerOnJava21() throws Exception {
        HttpClient httpClient = HttpClient.newHttpClient();
        Assume.assumeTrue(httpClient instanceof AutoCloseable);

        HttpProxyGetter getter = new HttpProxyGetter(
                httpClient,
                ClientPoolService.httpBuilder()
                        .setBalancerFqdn("localhost")
                        .setOptions(new RpcOptions())
        );

        getter.close();

        Object isTerminated = HttpClient.class
                .getMethod("awaitTermination", Duration.class)
                .invoke(httpClient, Duration.ofSeconds(30));
        assertTrue(isTerminated instanceof Boolean);
        assertTrue(
                "HttpClient did not terminate within 30 seconds",
                (Boolean) isTerminated
        );
    }

    @Test
    public void testClientPoolServiceClosesHttpProxyGetter() {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
        HttpProxyGetter getter;
        try (ClientPoolService clientPoolService = ClientPoolService.httpBuilder()
                .setDataCenterName("test")
                .setBalancerFqdn("localhost")
                .setOptions(new RpcOptions())
                .setClientFactory(new MockRpcClientFactory())
                .setEventLoop(eventLoopGroup)
                .setRandom(new Random())
                .build()) {
            getter = (HttpProxyGetter) clientPoolService.proxyGetter;
        } finally {
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
        assertTrue(getter.getProxies().isCompletedExceptionally());
    }
}
