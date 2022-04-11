package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.nio.NioEventLoopGroup;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.RequestBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.YtCluster;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static ru.yandex.yt.testlib.FutureUtils.getError;
import static ru.yandex.yt.testlib.FutureUtils.waitFuture;
import static ru.yandex.yt.testlib.FutureUtils.waitOkResult;
import static ru.yandex.yt.testlib.Matchers.isCausedBy;

public class TemporaryClusterErrorsTest {
    private BusConnector connector;

    @Before
    public void setup() {
        connector = new DefaultBusConnector(new NioEventLoopGroup(0));
    }

    static class WithBannedRpcProxies implements AutoCloseable {
        static class ListResponse {
            public List<String> value;
        }

        final AsyncHttpClient httpClient;
        final String ytAddress;
        List<String> proxyPathList = new ArrayList<>();
        final int HTTP_TIMEOUT = 5000;

        WithBannedRpcProxies(String ytAddress) {
            this.ytAddress = ytAddress;
            // NB. we are using http client, since we are going to ban proxies
            // and we cannot use banned proxies to unban themselves.
            httpClient = asyncHttpClient(
                    new DefaultAsyncHttpClientConfig.Builder()
                            .setHttpClientCodecMaxHeaderSize(65536)
                            .build());

            {
                var listUrl = String.format("http://%s/api/v4/list?path=//sys/rpc_proxies", ytAddress);
                var responseFuture = httpClient.executeRequest(
                        new RequestBuilder()
                                .setUrl(listUrl)
                                .build()).toCompletableFuture();
                waitOkResult(responseFuture, HTTP_TIMEOUT);

                var response = responseFuture.join();
                if (response.getStatusCode() != 200) {
                    throw new RuntimeException("Bad response: " + response);
                }
                final var objectMapper = new ObjectMapper();
                ListResponse listResponse;
                try {
                    listResponse = objectMapper.readValue(response.getResponseBody(), ListResponse.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                for (var proxyName : listResponse.value) {
                    var proxyPath = "//sys/rpc_proxies/" + proxyName;
                    proxyPathList.add(proxyPath);
                    setBanned(proxyPath, true);
                }
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
            var responseFuture = httpClient.executeRequest(
                    new RequestBuilder("PUT")
                            .setUrl(banUrl)
                            .setBody(value ? "true" : "false"))
                    .toCompletableFuture();
            waitOkResult(responseFuture, HTTP_TIMEOUT);
            var response = responseFuture.join();
            if (response.getStatusCode() != 200) {
                throw new RuntimeException("Bad response: " + response);
            }
        }
    }

    @Test
    public void testMultipleClusters() {
        final RpcCredentials credentials = new RpcCredentials("root", "");

        RpcOptions options = new RpcOptions();
        options.setGlobalTimeout(Duration.ofSeconds(1));
        options.setProxyUpdateTimeout(Duration.ofMillis(100));
        YtClient client = new YtClient(
                connector,
                List.of(new YtCluster(LocalYt.getAddress())),
                "localhost",
                credentials,
                options);

        waitOkResult(client.waitProxies(), 1000);

        var dataNode = YTree.stringNode("foo");
        var setFuture = client.setNode("//tmp/data-node", dataNode);
        waitOkResult(setFuture, 1000);

        {
            var getFuture = client.getNode("//tmp/data-node");
            waitOkResult(getFuture, 1000);
            assertThat(getFuture.join(), is(dataNode));
        }

        try (var ignored = new WithBannedRpcProxies(LocalYt.getAddress())) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            var getFuture = client.getNode("//tmp/data-node");
            waitFuture(getFuture, 2000);
            assertThat(getError(getFuture), isCausedBy(Exception.class));
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Proxy needs some time to understand that it's not banned anymore.
        for (int i = 0; i < 100; ++i) {
            try {
                var getFuture = client.getNode("//tmp/data-node");
                assertThat(getFuture.join(), is(dataNode));
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
