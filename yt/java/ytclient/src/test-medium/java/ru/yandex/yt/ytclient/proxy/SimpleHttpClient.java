package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.RequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.internal.HostPort;

import static org.asynchttpclient.Dsl.asyncHttpClient;

/**
 * Simple http client to work in those situations when all proxies are banned.
 */
public class SimpleHttpClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleHttpClient.class);

    final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    final AsyncHttpClient httpClient = asyncHttpClient(
                new DefaultAsyncHttpClientConfig.Builder()
                        .setThreadPoolName("YtClient-PeriodicDiscovery")
                        .setEventLoopGroup(eventLoopGroup)
                        .setHttpClientCodecMaxHeaderSize(65536)
                        .build()
        );

    final String proxyAddress;

    public SimpleHttpClient(HostPort proxyAddress) {
        this.proxyAddress = proxyAddress.getHost() + ":" + proxyAddress.getPort();
    }

    public CompletableFuture<Void> banProxy(String proxy, boolean value) {
        logger.debug("banProxy {} {}", proxy, value);
        var banUrl = String.format("http://%s/api/v4/set?path=//sys/rpc_proxies/%s/@banned", proxyAddress, proxy);
        return httpClient.executeRequest(
                new RequestBuilder("PUT")
                        .setUrl(banUrl)
                        .setBody(value ? "true" : "false"))
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != 200) {
                        throw new RuntimeException("Bad response: " + response);
                    }
                    return null;
                });
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }
}
