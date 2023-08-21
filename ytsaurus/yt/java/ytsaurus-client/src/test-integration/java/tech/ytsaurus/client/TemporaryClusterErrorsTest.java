package tech.ytsaurus.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.Assert.assertThat;
import static tech.ytsaurus.testlib.FutureUtils.getError;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;
import static tech.ytsaurus.testlib.FutureUtils.waitOkResult;
import static tech.ytsaurus.testlib.Matchers.isCausedBy;

public class TemporaryClusterErrorsTest extends YTsaurusClientTestBase {
    private BusConnector connector;

    @Before
    public void setup() {
        connector = new DefaultBusConnector(new NioEventLoopGroup(0));
    }

    static class WithBannedRpcProxies implements AutoCloseable {
        static class ListResponse {
            public List<String> value;
        }

        final HttpClient httpClient;
        final String ytAddress;
        List<String> proxyPathList = new ArrayList<>();
        final int httpTimeout = 5000;

        WithBannedRpcProxies(String ytAddress) {
            this.ytAddress = ytAddress;
            // NB. we are using http client, since we are going to ban proxies
            // and we cannot use banned proxies to unban themselves.
            httpClient = HttpClient.newBuilder().build();

            var listUrl = String.format("http://%s/api/v4/list?path=//sys/rpc_proxies", ytAddress);
            var responseFuture = httpClient.sendAsync(
                    HttpRequest.newBuilder(URI.create(listUrl)).build(), HttpResponse.BodyHandlers.ofString());
            waitOkResult(responseFuture, httpTimeout);

            var response = responseFuture.join();
            if (response.statusCode() != 200) {
                throw new RuntimeException("Bad response: " + response);
            }
            final var objectMapper = new ObjectMapper();
            ListResponse listResponse;
            try {
                listResponse = objectMapper.readValue(response.body(), ListResponse.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            for (var proxyName : listResponse.value) {
                var proxyPath = "//sys/rpc_proxies/" + proxyName;
                proxyPathList.add(proxyPath);
                setBanned(proxyPath, true);
            }
        }

        @Override
        public void close() {
            for (var proxyPath : proxyPathList) {
                setBanned(proxyPath, false);
            }
        }

        void setBanned(String proxy, boolean value) {
            var banUrl = String.format("http://%s/api/v4/set?path=%s/@banned", ytAddress, proxy);
            var responseFuture = httpClient.sendAsync(
                    HttpRequest.newBuilder(URI.create(banUrl))
                            .PUT(HttpRequest.BodyPublishers.ofString(value ? "true" : "false"))
                            .build(), HttpResponse.BodyHandlers.ofInputStream());
            waitOkResult(responseFuture, httpTimeout);
            var response = responseFuture.join();
            if (response.statusCode() != 200) {
                throw new RuntimeException("Bad response: " + response);
            }
        }
    }

    @Test
    public void testMultipleClusters() {
        final YTsaurusClientAuth auth = YTsaurusClientAuth.builder()
                .setUser("root")
                .setToken("")
                .build();

        RpcOptions options = new RpcOptions();
        options.setGlobalTimeout(Duration.ofSeconds(1));
        options.setProxyUpdateTimeout(Duration.ofMillis(100));
        var client = YTsaurusClient.builder()
                .setSharedBusConnector(connector)
                .setClusters(List.of(new YTsaurusCluster(getYTsaurusAddress())))
                .setPreferredClusterName("localhost")
                .setAuth(auth)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(options)
                                .build()
                )
                .disableValidation()
                .build();

        waitOkResult(client.waitProxies(), 2000);

        var dataNode = YTree.stringNode("foo");
        var setFuture = client.setNode("//tmp/data-node", dataNode);
        waitOkResult(setFuture, 2000);

        var getFuture = client.getNode("//tmp/data-node");
        waitOkResult(getFuture, 2000);
        Assert.assertEquals(getFuture.join(), dataNode);

        try (var ignored = new WithBannedRpcProxies(getYTsaurusAddress())) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            var getFutureAfterBan = client.getNode("//tmp/data-node");
            waitFuture(getFutureAfterBan, 2000);
            assertThat(getError(getFutureAfterBan), isCausedBy(Exception.class));
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Proxy needs some time to understand that it's not banned anymore.
        for (int i = 0; i < 100; ++i) {
            try {
                var getFutureAfterUnban = client.getNode("//tmp/data-node");
                Assert.assertEquals(getFutureAfterUnban.join(), dataNode);
                break;
            } catch (CompletionException ex) {
                try {
                    Thread.sleep(100, 0);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
