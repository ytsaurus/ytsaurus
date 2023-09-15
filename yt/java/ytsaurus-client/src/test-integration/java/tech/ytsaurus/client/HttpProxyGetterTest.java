package tech.ytsaurus.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.core.YtFormat;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class HttpProxyGetterTest extends YTsaurusClientTestBase {
    HttpClient httpClient;
    EventLoopGroup eventLoopGroup;

    @Before
    public void before() {
        eventLoopGroup = new NioEventLoopGroup(1);
        httpClient = HttpClient.newBuilder()
                .executor(eventLoopGroup)
                .build();
    }

    @After
    public void after() throws IOException {
        eventLoopGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
    }

    @Test
    public void testGetProxy() throws InterruptedException, ExecutionException, TimeoutException {
        HttpProxyGetter getter = new HttpProxyGetter(
                httpClient,
                ClientPoolService.httpBuilder()
                        .setBalancerFqdn(getYTsaurusHost())
                        .setBalancerPort(getYTsaurusPort())
        );
        final var hostPortComparator = Comparator
                .comparing(HostPort::getHost)
                .thenComparing(HostPort::getPort);

        var proxies = getter.getProxies();
        var proxiesFromGetter = proxies.get(2, TimeUnit.SECONDS)
                .stream()
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        var proxiesFromList = httpListRpcProxies(httpClient, getYTsaurusAddress())
                .stream()
                .map(HostPort::parse)
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        assertThat(proxiesFromGetter, is(proxiesFromList));
        assertThat(proxiesFromGetter.size(), greaterThan(0));
    }

    public static List<String> httpListRpcProxies(HttpClient httpClient, String address) {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(String.format("http://%s/api/v4/list?path=//sys" +
                            "/rpc_proxies", address)))
                    .setHeader("X-YT-Header-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .setHeader("X-YT-Output-Format", YTreeTextSerializer.serialize(YtFormat.YSON_TEXT))
                    .build();
            HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            assertThat(response.statusCode(), is(200));
            YTreeNode node = YTreeTextSerializer.deserialize(response.body());
            return node.mapNode()
                    .getOrThrow("value")
                    .asList().stream()
                    .map(YTreeNode::stringValue).collect(Collectors.toList());
        } catch (Throwable error) {
            throw new RuntimeException("Unexpected error", error);
        }
    }
}
