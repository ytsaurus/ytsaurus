package ru.yandex.yt.jdbc;

import java.util.Collections;
import java.util.Objects;

import io.netty.channel.nio.NioEventLoopGroup;
import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.YtCluster;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;


class YtClientWrapper {

    private final DefaultBusConnector bus;
    private final YtClient client;
    private final YtClientProperties properties;

    YtClientWrapper(YtClientProperties properties) {
        this.properties = Objects.requireNonNull(properties);

        // Use default thread count (2 x CPU) or -Dio.netty.eventLoopThreads=...
        this.bus = new DefaultBusConnector(new NioEventLoopGroup(Integer.getInteger("cpu.count", 0) * 2), true);

        final YtCluster cluster = new YtCluster(properties.getProxy());
        this.client = new YtClient(bus,
                Collections.singletonList(cluster),
                cluster.getName(),
                null,
                YTsaurusClientAuth.builder()
                        .setUser(properties.getUsername())
                        .setToken(properties.getToken())
                        .build(),
                new RpcCompression(properties.getCompression()),
                new RpcOptions()); // TODO: Поддержать настройки

        this.waitForProxies();
    }


    <T> T wrap(Class<T> interfaceClass, T object) {
        return properties.isDebugOutput() ? LogProxy.wrap(interfaceClass, object) : object;
    }

    void close() {
        try {
            client.close();
        } finally {
            bus.close();
        }
    }

    private void waitForProxies() {
        client.waitProxies().join(); // Make sure we connected
    }

    YtClient getClient() {
        return client;
    }

    YtClientProperties getProperties() {
        return properties;
    }
}
