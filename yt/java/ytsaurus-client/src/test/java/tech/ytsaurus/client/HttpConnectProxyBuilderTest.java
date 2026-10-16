package tech.ytsaurus.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;
import tech.ytsaurus.client.bus.Bus;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.BusListener;
import tech.ytsaurus.client.bus.BusServer;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class HttpConnectProxyBuilderTest {
    private static final String PROXY_HOST = "127.0.0.1";
    private static final int PROXY_PORT = 3128;

    private static YTsaurusClient.Builder builder() {
        YTsaurusClient.Builder builder = new YTsaurusClient.Builder();
        builder.setAuth(YTsaurusClientAuth.empty());
        return builder;
    }

    @Test
    public void proxyIsAppliedToDefaultConnector() {
        YTsaurusClient.Builder builder = builder();
        builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

        BusConnector connector = new YTsaurusClient.BuilderWithDefaults<>(builder).busConnector;
        try {
            assertEquals(new InetSocketAddress(PROXY_HOST, PROXY_PORT),
                    ((DefaultBusConnector) connector).getHttpConnectProxy());
        } finally {
            connector.close();
        }
    }

    @Test
    public void proxyIsAppliedToOwnConnector() {
        DefaultBusConnector own = new DefaultBusConnector();
        try {
            YTsaurusClient.Builder builder = builder();
            builder.setOwnBusConnector(own);
            builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

            new YTsaurusClient.BuilderWithDefaults<>(builder);

            assertEquals(new InetSocketAddress(PROXY_HOST, PROXY_PORT), own.getHttpConnectProxy());
        } finally {
            own.close();
        }
    }

    @Test
    public void sharedConnectorIsNotReconfigured() {
        DefaultBusConnector shared = new DefaultBusConnector();
        try {
            YTsaurusClient.Builder builder = builder();
            builder.setSharedBusConnector(shared);
            builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

            assertThrows(IllegalArgumentException.class, () -> new YTsaurusClient.BuilderWithDefaults<>(builder));
            assertNull(shared.getHttpConnectProxy());
        } finally {
            shared.close();
        }
    }

    @Test
    public void sharedConnectorWithTheSameProxyIsAccepted() {
        DefaultBusConnector shared = new DefaultBusConnector();
        try {
            shared.setHttpConnectProxy(new InetSocketAddress(PROXY_HOST, PROXY_PORT));
            YTsaurusClient.Builder builder = builder();
            builder.setSharedBusConnector(shared);
            builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

            new YTsaurusClient.BuilderWithDefaults<>(builder);

            assertEquals(new InetSocketAddress(PROXY_HOST, PROXY_PORT), shared.getHttpConnectProxy());
        } finally {
            shared.close();
        }
    }

    @Test
    public void sharedConnectorWithAnotherProxyIsRejected() {
        DefaultBusConnector shared = new DefaultBusConnector();
        try {
            shared.setHttpConnectProxy(new InetSocketAddress(PROXY_HOST, PROXY_PORT + 1));
            YTsaurusClient.Builder builder = builder();
            builder.setSharedBusConnector(shared);
            builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

            assertThrows(IllegalArgumentException.class, () -> new YTsaurusClient.BuilderWithDefaults<>(builder));
        } finally {
            shared.close();
        }
    }

    @Test
    public void proxiedBusWithDirectDiscoveryIsRejected() {
        DefaultBusConnector shared = new DefaultBusConnector();
        try {
            shared.setHttpConnectProxy(new InetSocketAddress(PROXY_HOST, PROXY_PORT));

            // Http discovery would go directly while the bus goes through the proxy.
            assertThrows(IllegalArgumentException.class, () -> new YTsaurusClient.ClientPoolProvider(
                    shared,
                    List.of(new YTsaurusCluster("hume")),
                    null,
                    null,
                    null,
                    null,
                    false,
                    false,
                    false,
                    YTsaurusClientAuth.empty(),
                    (hostPort, name) -> {
                        throw new UnsupportedOperationException();
                    },
                    new RpcOptions(),
                    Runnable::run));
        } finally {
            shared.close();
        }
    }

    @Test
    public void unresolvableProxyHostIsRejected() {
        // Fail fast on a typo instead of failing every connection later.
        assertThrows(IllegalArgumentException.class,
                () -> builder().setHttpConnectProxy("yt-proxy.invalid", PROXY_PORT));
    }

    @Test
    public void unsupportedConnectorIsRejected() {
        StubBusConnector custom = new StubBusConnector();
        try {
            YTsaurusClient.Builder builder = builder();
            builder.setOwnBusConnector(custom);
            builder.setHttpConnectProxy(PROXY_HOST, PROXY_PORT);

            assertThrows(IllegalArgumentException.class, () -> new YTsaurusClient.BuilderWithDefaults<>(builder));
        } finally {
            custom.close();
        }
    }

    private static final class StubBusConnector implements BusConnector {
        private final EventLoopGroup group = new NioEventLoopGroup(1);

        @Override
        public Bus connect(SocketAddress address, BusListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BusServer listen(SocketAddress address, BusListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventLoopGroup eventLoopGroup() {
            return group;
        }

        @Override
        public void close() {
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }
}
