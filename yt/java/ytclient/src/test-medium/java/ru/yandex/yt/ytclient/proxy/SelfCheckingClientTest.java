package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.testlib.FutureUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.Compression;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;


@RunWith(Parameterized.class)
public class SelfCheckingClientTest extends YtClientTestBase {
    @Parameterized.Parameter
    public RpcCompression rpcCompression;

    @Parameterized.Parameters
    public static Object[] parameters() {
        return new Object[][]{
                {new RpcCompression()},
                {new RpcCompression(Compression.Zlib_8)},
        };
    }

    @Test
    public void test() {
        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setNewDiscoveryServiceEnabled(true);
        var ytFixture = createYtFixture(rpcOptions);
        var yt = ytFixture.yt;

        var sysRpcProxies = YPath.simple("//sys/rpc_proxies");
        var rpcProxies = yt.listNode(sysRpcProxies.toString()).join().asList();
        var proxyAddress = rpcProxies.get(0).stringValue();

        BusConnector busConnector = new DefaultBusConnector();
        try (busConnector) {
            var rawClient = new DefaultRpcBusClient(busConnector, HostPort.parse(proxyAddress).toInetSocketAddress())
                    .withCompression(rpcCompression);

            var statusFuture = new CompletableFuture<Void>();
            var options = new RpcOptions();
            var selfCheckingClient = new SelfCheckingClient(rawClient, options, statusFuture);

            assertThrows(RuntimeException.class, () -> FutureUtils.waitFuture(statusFuture, 1000));

            var httpClient = new SimpleHttpClient(ytFixture.address);
            httpClient.banProxy(proxyAddress, true).join();
            try {
                FutureUtils.waitFuture(statusFuture, 50000);
                assertThat(FutureUtils.getError(statusFuture).toString(),
                        containsString("Proxy is down"));
            } finally {
                httpClient.banProxy(proxyAddress, false).join();

                // Proxy needs some time to understand that it's not banned anymore.
                for (int i = 0; i < 100; ++i) {
                    try {
                        yt.listNode("/").join();
                        break;
                    } catch (CompletionException ex) {
                        try {
                            Thread.sleep(100, 0);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            }

            selfCheckingClient.close();
        }
    }
}
