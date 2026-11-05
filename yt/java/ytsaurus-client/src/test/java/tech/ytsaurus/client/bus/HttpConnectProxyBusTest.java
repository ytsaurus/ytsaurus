package tech.ytsaurus.client.bus;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.ProxyConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpConnectProxyBusTest {
    // RFC 6761 guarantees that .invalid names never resolve.
    private static final String UNRESOLVABLE_HOST = "yt-proxy.invalid";

    private DefaultBusConnector serverConnector;
    private DefaultBusConnector clientConnector;
    private ConnectProxy proxy;

    @Before
    public void createConnectors() {
        serverConnector = new DefaultBusConnector()
                .setReadTimeout(Duration.ofSeconds(5))
                .setWriteTimeout(Duration.ofSeconds(5));
        clientConnector = new DefaultBusConnector()
                .setReadTimeout(Duration.ofSeconds(5))
                .setWriteTimeout(Duration.ofSeconds(5));
    }

    @After
    public void stopAll() {
        try {
            if (proxy != null) {
                proxy.close();
                proxy = null;
            }
        } finally {
            try {
                if (clientConnector != null) {
                    clientConnector.close();
                    clientConnector = null;
                }
            } finally {
                if (serverConnector != null) {
                    serverConnector.close();
                    serverConnector = null;
                }
            }
        }
    }

    @Test
    public void roundtripThroughProxy() throws InterruptedException, ExecutionException, TimeoutException {
        BlockingQueue<Object> serverQueue = new ArrayBlockingQueue<>(1);
        BusServer server = serverConnector.listen(new InetSocketAddress("127.0.0.1", 0), new BusListenerAdapter() {
            @Override
            public void onMessage(Bus bus, List<byte[]> message) {
                try {
                    assertEquals("Hello, world!", new String(message.get(0)));
                    bus.send(Collections.singletonList("Message received".getBytes()), BusDeliveryTracking.NONE);
                    bus.close();
                } catch (Throwable e) {
                    serverQueue.add(e);
                    throw e;
                }
                serverQueue.add("OK");
            }
        });
        server.bound().sync();
        int serverPort = ((InetSocketAddress) server.localAddress()).getPort();

        proxy = new ConnectProxy(new InetSocketAddress("127.0.0.1", serverPort));
        clientConnector.setHttpConnectProxy(new InetSocketAddress("127.0.0.1", proxy.port()));

        BlockingQueue<Object> clientQueue = new ArrayBlockingQueue<>(1);
        // The hostname does not resolve locally: only the proxy is expected to resolve it.
        Bus client = clientConnector.connect(
                new InetSocketAddress(UNRESOLVABLE_HOST, serverPort),
                new BusListenerAdapter() {
                    @Override
                    public void onMessage(Bus bus, List<byte[]> message) {
                        try {
                            assertEquals("Message received", new String(message.get(0)));
                            bus.close();
                        } catch (Throwable e) {
                            clientQueue.add(e);
                            throw e;
                        }
                        clientQueue.add("OK");
                    }
                });
        client.connected().sync();

        client.send(Collections.singletonList("Hello, world!".getBytes()), BusDeliveryTracking.FULL)
                .get(5, TimeUnit.SECONDS);

        assertEquals("OK", serverQueue.poll(5, TimeUnit.SECONDS));
        assertEquals("OK", clientQueue.poll(5, TimeUnit.SECONDS));
        // The proxy received CONNECT by hostname (the address was not resolved on the client).
        assertEquals(UNRESOLVABLE_HOST + ":" + serverPort, proxy.awaitConnectTarget());
    }

    @Test
    public void requestThroughRejectedTunnelReportsProxyStatus() throws InterruptedException {
        proxy = new ConnectProxy(null);
        clientConnector.setHttpConnectProxy(new InetSocketAddress("127.0.0.1", proxy.port()));

        BlockingQueue<Throwable> reported = new ArrayBlockingQueue<>(4);
        Bus client = clientConnector.connect(
                new InetSocketAddress(UNRESOLVABLE_HOST, 65000), new BusListenerAdapter() {
                    @Override
                    public void onException(Bus bus, Throwable cause) {
                        reported.add(cause);
                    }
                });

        CompletableFuture<Void> request =
                client.send(Collections.singletonList("ping".getBytes()), BusDeliveryTracking.FULL);
        try {
            request.get(5, TimeUnit.SECONDS);
            fail("a request through a rejected CONNECT tunnel must fail");
        } catch (ExecutionException expected) {
            // The tunnel was rejected, so the request fails instead of hanging.
        } catch (TimeoutException e) {
            fail("a request through a rejected CONNECT tunnel neither completed nor failed");
        }

        Throwable proxyFailure = reported.poll(5, TimeUnit.SECONDS);
        assertTrue("the proxy status must be reported, got: " + proxyFailure,
                proxyFailure instanceof ProxyConnectException && proxyFailure.getMessage().contains("403"));
    }

    /**
     * Minimal in-process HTTP CONNECT proxy. Tunnels bytes to {@code upstream};
     * if {@code upstream == null}, rejects the tunnel with 403 as a squid with a restricted
     * port ACL does.
     */
    private static final class ConnectProxy implements AutoCloseable {
        private final EventLoopGroup group = new NioEventLoopGroup(1);
        private final Channel serverChannel;
        private final InetSocketAddress upstream;
        private final CompletableFuture<String> connectTarget = new CompletableFuture<>();

        ConnectProxy(InetSocketAddress upstream) throws InterruptedException {
            this.upstream = upstream;
            serverChannel = new ServerBootstrap()
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline().addLast(new FrontHandler());
                        }
                    })
                    .bind(new InetSocketAddress("127.0.0.1", 0))
                    .sync()
                    .channel();
        }

        int port() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        String awaitConnectTarget() {
            try {
                return connectTarget.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            serverChannel.close().syncUninterruptibly();
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }

        private static int findDoubleCrlf(ByteBuf buf) {
            for (int i = buf.readerIndex(); i <= buf.writerIndex() - 4; i++) {
                if (buf.getByte(i) == '\r' && buf.getByte(i + 1) == '\n'
                        && buf.getByte(i + 2) == '\r' && buf.getByte(i + 3) == '\n') {
                    return i;
                }
            }
            return -1;
        }

        private final class FrontHandler extends ChannelInboundHandlerAdapter {
            private final ByteBuf accumulator = Unpooled.buffer();
            private Channel upstreamChannel;
            private boolean tunnelReady;

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ByteBuf in = (ByteBuf) msg;
                if (tunnelReady) {
                    upstreamChannel.writeAndFlush(in);
                    return;
                }
                accumulator.writeBytes(in);
                in.release();

                int headerStart = accumulator.readerIndex();
                int headerEnd = findDoubleCrlf(accumulator);
                if (headerEnd < 0) {
                    return;
                }
                String header = accumulator.toString(headerStart, headerEnd - headerStart, StandardCharsets.US_ASCII);
                accumulator.readerIndex(headerEnd + 4);
                connectTarget.complete(header.split("\r\n", 2)[0].split(" ")[1]);

                if (upstream == null) {
                    ctx.writeAndFlush(Unpooled.copiedBuffer(
                                    "HTTP/1.1 403 Forbidden\r\n\r\n", StandardCharsets.US_ASCII))
                            .addListener(ChannelFutureListener.CLOSE);
                    return;
                }
                openTunnel(ctx);
            }

            private void openTunnel(ChannelHandlerContext ctx) {
                new Bootstrap()
                        .group(ctx.channel().eventLoop())
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInboundHandlerAdapter() {
                            @Override
                            public void channelRead(ChannelHandlerContext upCtx, Object m) {
                                ctx.channel().writeAndFlush(m);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext upCtx) {
                                ctx.close();
                            }
                        })
                        .connect(upstream)
                        .addListener((ChannelFuture f) -> {
                            if (!f.isSuccess()) {
                                ctx.close();
                                return;
                            }
                            upstreamChannel = f.channel();
                            tunnelReady = true;
                            ctx.writeAndFlush(Unpooled.copiedBuffer(
                                    "HTTP/1.1 200 Connection established\r\n\r\n", StandardCharsets.US_ASCII));
                            if (accumulator.isReadable()) {
                                upstreamChannel.writeAndFlush(accumulator.readBytes(accumulator.readableBytes()));
                            }
                        });
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                if (accumulator.refCnt() > 0) {
                    accumulator.release();
                }
                if (upstreamChannel != null) {
                    upstreamChannel.close();
                }
            }
        }
    }
}
