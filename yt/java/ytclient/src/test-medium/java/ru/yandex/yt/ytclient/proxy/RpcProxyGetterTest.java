package ru.yandex.yt.ytclient.proxy;

import java.io.IOException;
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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactory;
import ru.yandex.yt.ytclient.proxy.internal.RpcClientFactoryImpl;
import ru.yandex.yt.ytclient.rpc.RpcClientPool;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RpcProxyGetterTest {
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
                        new RpcCredentials(),
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
                        new RpcCredentials(),
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
