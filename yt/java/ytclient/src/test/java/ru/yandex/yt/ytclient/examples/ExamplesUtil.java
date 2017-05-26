package ru.yandex.yt.ytclient.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.function.Consumer;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusFactory;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public final class ExamplesUtil {
    private static final String YT_HOST = "barney.yt.yandex.net";
    private static final int YT_PORT = 9013;

    private ExamplesUtil() {
        // static class
    }

    public static BusConnector createConnector() {
        return new DefaultBusConnector();
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

    public static RpcClient createRpcClient(BusConnector connector, String user, String token) {
        RpcClient client = new DefaultRpcBusClient(
                new DefaultBusFactory(connector, () -> new InetSocketAddress(YT_HOST, YT_PORT)));
        return client.withTokenAuthentication(user, token);
    }

    public static void runExample(Consumer<ApiServiceClient> consumer, String user, String token) {
        try (BusConnector connector = createConnector()) {
            try (RpcClient rpcClient = createRpcClient(connector, user, token)) {
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));
                consumer.accept(serviceClient);
            }
        }
    }

    public static void runExample(Consumer<ApiServiceClient> consumer) {
        runExample(consumer, getUser(), getToken());
    }
}
