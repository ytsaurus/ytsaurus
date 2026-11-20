package tech.ytsaurus.client;

import java.net.http.HttpClient;
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

        // A terminated HttpClient has stopped its selector manager and released its connections.
        Object isTerminated = HttpClient.class.getMethod("isTerminated").invoke(httpClient);
        assertTrue(isTerminated instanceof Boolean);
        assertTrue((Boolean) isTerminated);
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
