package ru.yandex.yt.ytclient.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import io.netty.channel.nio.NioEventLoopGroup;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.YtCluster;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.Compression;

public final class ExamplesUtil {
    // get this list from //sys/rpc_proxies
    // see YT-7594
    private static final String[] hosts = new String[] {
            "man2-4291-d59.hume.yt.gencfg-c.yandex.net"
//            "man2-3929-d7d.socrates.yt.gencfg-c.yandex.net"
//            "man2-4179-c03.hume.yt.gencfg-c.yandex.net",
//        "n0026-sas.hahn.yt.yandex.net",
//        "n0028-sas.hahn.yt.yandex.net",
//        "n0045-sas.hahn.yt.yandex.net",
//        "n0046-sas.hahn.yt.yandex.net"
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

    static private boolean CompressionEnabled = false;

    public static void enableCompression() {
        CompressionEnabled = true;
    }

    public static RpcClient createRpcClient(BusConnector connector, RpcCredentials credentials, String host, int port) {
        RpcClient client = new DefaultRpcBusClient(connector, new InetSocketAddress(host, port));

        if (CompressionEnabled) {
            return client
                    .withTokenAuthentication(credentials)
                    .withCompression(new RpcCompression(Compression.Zlib_6));
        } else {
            return client.withTokenAuthentication(credentials);
        }
    }

    public static RpcClient createRpcClient(BusConnector connector, RpcCredentials credentials, String host, int port, String shortName) {
        RpcClient client = new DefaultRpcBusClient(connector, new InetSocketAddress(host, port), shortName);
        if (CompressionEnabled) {
            return client
                    .withTokenAuthentication(credentials)
                    .withCompression(new RpcCompression(Compression.Zlib_6));
        } else {
            return client.withTokenAuthentication(credentials);
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, RpcCredentials credentials) {
        runExample(consumer, credentials, getRandomHost());
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, RpcCredentials credentials, String host) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createRpcClient(connector, credentials, host, YT_PORT)) {
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setGlobalTimeout(Duration.ofSeconds(15)));
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
            client = new YtClient(
                    connector,
                    List.of(new YtCluster("hume")),
                    "dc",
                    null,
                    getCredentials(),
                    new RpcCompression(Compression.Lz4),
                    new RpcOptions());
            client.waitProxies().join();
            consumer.accept(client);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
                System.exit(0);
            }
        }
    }
}
