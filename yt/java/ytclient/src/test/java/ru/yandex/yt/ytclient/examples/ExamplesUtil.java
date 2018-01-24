package ru.yandex.yt.ytclient.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.nio.NioEventLoopGroup;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusFactory;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.DefaultRpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public final class ExamplesUtil {
    // get this list from //sys/rpc_proxies
    // see YT-7594
    private static final String[] hosts = new String[] {
        "n0026-sas.hahn.yt.yandex.net",
        "n0028-sas.hahn.yt.yandex.net",
        "n0045-sas.hahn.yt.yandex.net",
        "n0046-sas.hahn.yt.yandex.net"
    };
    private static final int YT_PORT = 9013;
    private static final Random random = new Random();

    private ExamplesUtil() {
        // static class
    }

    public static BusConnector createConnector() {
        return createConnector(0);
    }

    public static BusConnector createConnector(int threads) {
        return new DefaultBusConnector(new NioEventLoopGroup(threads), true);
    }

    public static String getUser() {
        return System.getProperty("user.name");
    }

    public static String getToken() {
        String token;
        try {
            Path tokenPath = Paths.get(System.getProperty("user.home"), ".yt", "token");
            try (BufferedReader reader = Files.newBufferedReader(tokenPath)) {
                token = reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (token == null) {
            throw new IllegalStateException("Missing user yt token");
        }
        return token;
    }

    static String getRandomHost() {
        return hosts[random.nextInt(hosts.length)];
    }

    public static RpcClient createRpcClient(BusConnector connector, String user, String token) {
        return createRpcClient(connector, user, token, getRandomHost(), YT_PORT);
    }

    public static RpcClient createRpcClient(BusConnector connector, String user, String token, String host, int port) {
        RpcClient client = new DefaultRpcBusClient(
                new DefaultBusFactory(connector, () -> new InetSocketAddress(host, port)));
        return client.withTokenAuthentication(user, token);
    }

    public static RpcClient createRpcClient(BusConnector connector, String user, String token, String host, int port, String shortName) {
        RpcClient client = new DefaultRpcBusClient(
            new DefaultBusFactory(connector, () -> new InetSocketAddress(host, port)), shortName);
        return client.withTokenAuthentication(user, token);
    }

    public static RpcClient createBalancingRpcClient(BusConnector connector, String user, String token)
    {
        List<RpcClient> clients = Arrays.asList(hosts).stream()
                .map(host -> createRpcClient(connector, user, token, host, YT_PORT, host))
                .collect(Collectors.toList());

        RpcClient rpcClient = new BalancingRpcClient(
                Duration.ofMillis(60),  // sends fallback request to other proxy after this timeout
                Duration.ofMillis(5000), // fails request after this timeout
                Duration.ofSeconds(1),  // marks proxy as dead/live after this timeout
                connector,
                new DefaultRpcFailoverPolicy(),
                "man", // our local datacenter
                ImmutableMap.of(
                        "man", // cluster datacenter
                        clients)
        );

        return rpcClient;
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, String user, String token) {
        runExample(consumer, user, token, getRandomHost());
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, String user, String token, String host) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createRpcClient(connector, user, token, host, YT_PORT)) {
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));
                consumer.accept(serviceClient);
            }
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer) {
        runExample(consumer, getUser(), getToken());
    }

    public static void runExampleWithBalancing(Consumer<ApiServiceClient> consumer) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createBalancingRpcClient(connector, getUser(), getToken())) {
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));
                consumer.accept(serviceClient);
            }
        }
    }
}
