package tech.ytsaurus.client;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcClientPool;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import ru.yandex.yt.testlib.LocalYt;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RpcProxyGetterTest {
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
        eventLoopGroup.shutdownGracefully(100, 100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void getProxiesInitial() throws InterruptedException, ExecutionException, TimeoutException {
        final var hostPortComparator = Comparator
                .comparing(HostPort::getHost)
                .thenComparing(HostPort::getPort);

        final var proxiesFromList = HttpProxyGetterTest.httpListRpcProxies(httpClient, LocalYt.getAddress())
                .stream()
                .map(HostPort::parse)
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        if (proxiesFromList.isEmpty()) {
            throw new RuntimeException("Local YT returned empty list of rpc proxies :(");
        }

        RpcProxyGetter proxyGetter = new RpcProxyGetter(
                List.of(proxiesFromList.get(0)),
                null,
                null,
                "local",
                new RpcClientFactoryImpl(
                        new DefaultBusConnector(),
                        YTsaurusClientAuth.empty(),
                        new RpcCompression()),
                new RpcOptions(),
                new Random());

        var proxiesFromGetter = proxyGetter.getProxies()
                .get(100, TimeUnit.MILLISECONDS)
                .stream().sorted(hostPortComparator).collect(Collectors.toList());

        assertThat(proxiesFromGetter, is(proxiesFromList));
    }

    @Test
    public void getProxiesPool() throws InterruptedException, ExecutionException, TimeoutException {
        final var hostPortComparator = Comparator
                .comparing(HostPort::getHost)
                .thenComparing(HostPort::getPort);

        final var proxiesFromList = HttpProxyGetterTest.httpListRpcProxies(httpClient, LocalYt.getAddress())
                .stream()
                .map(HostPort::parse)
                .sorted(hostPortComparator)
                .collect(Collectors.toList());

        RpcClientFactory rpcClientFactory = new RpcClientFactoryImpl(
                new DefaultBusConnector(),
                YTsaurusClientAuth.empty(),
                new RpcCompression());
        var poolClient = rpcClientFactory.create(proxiesFromList.get(0), "local-dc");
        RpcProxyGetter proxyGetter = new RpcProxyGetter(
                List.of(HostPort.parse("example.com")),
                RpcClientPool.collectionPool(Stream.generate(() -> poolClient)),
                null,
                "local",
                rpcClientFactory,
                new RpcOptions(),
                new Random());

        var proxiesFromGetter = proxyGetter.getProxies()
                .get(100, TimeUnit.MILLISECONDS)
                .stream().sorted(hostPortComparator).collect(Collectors.toList());

        assertThat(proxiesFromGetter, is(proxiesFromList));
    }
}
