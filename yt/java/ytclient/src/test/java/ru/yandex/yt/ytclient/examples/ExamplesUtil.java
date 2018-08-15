package ru.yandex.yt.ytclient.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Random;
import java.util.function.Consumer;

import io.netty.channel.nio.NioEventLoopGroup;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusFactory;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
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

    public static String[] getHosts() {
        return hosts;
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

    public static RpcCredentials getCredentials()
    {
        return new RpcCredentials(getUser(), getToken());
    }

    static String getRandomHost() {
        return hosts[random.nextInt(hosts.length)];
    }

    public static RpcClient createRpcClient(BusConnector connector, RpcCredentials credentials) {
        return createRpcClient(connector, credentials, getRandomHost(), YT_PORT);
    }

    public static RpcClient createRpcClient(BusConnector connector, RpcCredentials credentials, String host, int port) {
        RpcClient client = new DefaultRpcBusClient(
                new DefaultBusFactory(connector, () -> new InetSocketAddress(host, port)));
        return client.withTokenAuthentication(credentials);
    }

    public static RpcClient createRpcClient(BusConnector connector, RpcCredentials credentials, String host, int port, String shortName) {
        RpcClient client = new DefaultRpcBusClient(
            new DefaultBusFactory(connector, () -> new InetSocketAddress(host, port)), shortName);
        return client.withTokenAuthentication(credentials);
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, RpcCredentials credentials) {
        runExample(consumer, credentials, getRandomHost());
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, RpcCredentials credentials, String host) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createRpcClient(connector, credentials, host, YT_PORT)) {
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));
                consumer.accept(serviceClient);
            }
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer) {
        runExample(consumer, getCredentials());
    }

    public static void runExampleWithBalancing(Consumer<YtClient> consumer) {
        YtClient client = null;
        try (BusConnector connector = createConnector()) {
            client = new YtClient(connector, "hahn", getCredentials());
            client.waitProxies().join();
            consumer.accept(client);
        } finally {
            if (client != null) {
                client.close();
                System.exit(0);
            }
        }
    }
}
