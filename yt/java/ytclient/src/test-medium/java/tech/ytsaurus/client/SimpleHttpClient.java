package tech.ytsaurus.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple http client to work in those situations when all proxies are banned.
 */
public class SimpleHttpClient {
    private static final Logger logger = LoggerFactory.getLogger(SimpleHttpClient.class);

    final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    final HttpClient httpClient = HttpClient.newBuilder()
            .executor(eventLoopGroup)
                .build();

    final String proxyAddress;

    public SimpleHttpClient(HostPort proxyAddress) {
        this.proxyAddress = proxyAddress.getHost() + ":" + proxyAddress.getPort();
    }

    public CompletableFuture<Void> banProxy(String proxy, boolean value) {
        logger.debug("banProxy {} {}", proxy, value);
        var banUrl = String.format("http://%s/api/v4/set?path=//sys/rpc_proxies/%s/@banned", proxyAddress, proxy);
        return httpClient.sendAsync(
                HttpRequest.newBuilder(URI.create(banUrl))
                        .PUT(HttpRequest.BodyPublishers.ofString(value ? "true" : "false"))
                        .build(), HttpResponse.BodyHandlers.ofInputStream())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        throw new RuntimeException("Bad response: " + response);
                    }
                    return null;
                });
    }
}
