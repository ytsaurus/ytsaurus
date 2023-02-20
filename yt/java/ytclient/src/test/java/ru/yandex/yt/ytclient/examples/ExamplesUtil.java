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
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import io.netty.channel.nio.NioEventLoopGroup;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.ApiServiceClientImpl;
import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.YtClientConfiguration;
import tech.ytsaurus.client.YtCluster;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.DefaultRpcBusClient;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import ru.yandex.yt.ytclient.proxy.YandexSerializationResolver;

public final class ExamplesUtil {
    // get this list from //sys/rpc_proxies
    // see YT-7594
    private static final String[] HOSTS = new String[]{
            "man2-4291-d59.hume.yt.gencfg-c.yandex.net"
//            "man2-3929-d7d.socrates.yt.gencfg-c.yandex.net"
//            "man2-4179-c03.hume.yt.gencfg-c.yandex.net",
//        "n0026-sas.hahn.yt.yandex.net",
//        "n0028-sas.hahn.yt.yandex.net",
//        "n0045-sas.hahn.yt.yandex.net",
//        "n0046-sas.hahn.yt.yandex.net"
    };
    private static final int YT_PORT = 9013;
    private static final Random RANDOM = new Random();
    private static boolean compressionEnabled = false;

    private ExamplesUtil() {
        // static class
    }

    public static String[] getHosts() {
        return HOSTS;
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

    public static YTsaurusClientAuth getClientAuth() {
        return YTsaurusClientAuth.builder()
                .setUser(getUser())
                .setToken(getToken())
                .build();
    }

    static String getRandomHost() {
        return HOSTS[RANDOM.nextInt(HOSTS.length)];
    }

    public static RpcClient createRpcClient(BusConnector connector, YTsaurusClientAuth auth) {
        return createRpcClient(connector, auth, getRandomHost(), YT_PORT);
    }

    public static void enableCompression() {
        compressionEnabled = true;
    }

    public static RpcClient createRpcClient(BusConnector connector, YTsaurusClientAuth auth, String host,
                                            int port) {
        RpcClient client = new DefaultRpcBusClient(connector, new InetSocketAddress(host, port));

        if (compressionEnabled) {
            return client
                    .withAuthentication(auth)
                    .withCompression(new RpcCompression(Compression.Zlib_6));
        } else {
            return client.withAuthentication(auth);
        }
    }

    public static RpcClient createRpcClient(BusConnector connector,
                                            YTsaurusClientAuth auth,
                                            String host,
                                            int port,
                                            String shortName) {
        RpcClient client = new DefaultRpcBusClient(connector, new InetSocketAddress(host, port), shortName);
        if (compressionEnabled) {
            return client
                    .withAuthentication(auth)
                    .withCompression(new RpcCompression(Compression.Zlib_6));
        } else {
            return client.withAuthentication(auth);
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, YTsaurusClientAuth auth) {
        runExample(consumer, auth, getRandomHost());
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, YTsaurusClientAuth auth, String host) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createRpcClient(connector, auth, host, YT_PORT)) {
                ApiServiceClient serviceClient = new ApiServiceClientImpl(
                        rpcClient,
                        YtClientConfiguration.builder()
                                .setRpcOptions(
                                        new RpcOptions().setGlobalTimeout(Duration.ofSeconds(15))
                                ).build(),
                        ForkJoinPool.commonPool(),
                        connector.executorService(),
                        YandexSerializationResolver.getInstance());
                consumer.accept(serviceClient);
            }
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer) {
        runExample(consumer, getClientAuth());
    }

    public static void runExampleWithBalancing(Consumer<YtClient> consumer) {
        YtClient client = null;
        try (BusConnector connector = createConnector()) {
            client = new YtClient(
                    connector,
                    List.of(new YtCluster("hume")),
                    "dc",
                    null,
                    getClientAuth(),
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
