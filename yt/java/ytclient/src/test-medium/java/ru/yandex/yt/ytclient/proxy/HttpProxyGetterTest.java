package ru.yandex.yt.ytclient.proxy;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.common.YtFormat;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class HttpProxyGetterTest {
    AsyncHttpClient httpClient;
    EventLoopGroup eventLoopGroup;

    @Before
    public void before() {
        eventLoopGroup = new NioEventLoopGroup(1);
        httpClient = asyncHttpClient(
                new DefaultAsyncHttpClientConfig.Builder()
                        .setThreadPoolName("YtClient-PeriodicDiscovery")
                        .setEventLoopGroup(eventLoopGroup)
                        .setHttpClientCodecMaxHeaderSize(65536)
                        .build()
        );
    }

    @After
    public void after() throws IOException {
        httpClient.close();
        eventLoopGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
    }

    @Test
    public void testGetProxy() throws InterruptedException, ExecutionException, TimeoutException {
        HttpProxyGetter getter = new HttpProxyGetter(
                httpClient,
                LocalYt.getAddress(),
                null
        );
        final var hostPortComparator = Comparator
                .comparing(HostPort::getHost)
                .thenComparing(HostPort::getPort);

        var proxies = getter.getProxies();
        var proxiesFromGetter = proxies.get(2, TimeUnit.SECONDS)
                .stream()
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        var proxiesFromList = httpListRpcProxies(httpClient, LocalYt.getAddress())
                .stream()
                .map(HostPort::parse)
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        assertThat(proxiesFromGetter, is(proxiesFromList));
        assertThat(proxiesFromGetter.size(), greaterThan(0));
    }

    public static List<String> httpListRpcProxies(AsyncHttpClient httpClient, String address) {
        try {
            Request request = new RequestBuilder()
                    .setUrl(String.format("http://%s/api/v4/list?path=//sys/rpc_proxies", address))
                    .setHeader("X-YT-Header-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .setHeader("X-YT-Output-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .build();
            CompletableFuture<Response> responseFuture = httpClient.executeRequest(request).toCompletableFuture();
            var response = responseFuture.get(2, TimeUnit.SECONDS);

            assertThat(response.getStatusCode(), is(200));
            YTreeNode node = YTreeTextSerializer.deserialize(response.getResponseBodyAsStream());
            return node.mapNode()
                    .getOrThrow("value")
                    .asList().stream()
                    .map(YTreeNode::stringValue).collect(Collectors.toList());
        } catch (Throwable error) {
            throw new RuntimeException("Unexpected error", error);
        }
    }
}
