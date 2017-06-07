package ru.yandex.yt.rpc.channel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ru.yandex.yt.rpc.handlers.ChannelPoolHandler;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;

/**
 * @author valri
 */
public class ClientChannelPool {
    private static Logger logger = LogManager.getLogger(ClientChannelPool.class);

    private EventLoopGroup group = new NioEventLoopGroup(0);
    private ChannelPool channelPool;

    /**
     * Creates instance of channel pool for communication with RPC server.
     *
     * @param address            RPC server remote address
     * @param channelReadTimeout read timeout for each channel
     */
    public ClientChannelPool(InetSocketAddress address, int channelReadTimeout) {
        this(address, new ClientChannelInitializer(channelReadTimeout));
    }

    /**
     * Creates instance of channel pool for communication with RPC server.
     *
     * @param address            RPC server remote address
     * @param channelInitializer channel initializer with pipeline for each channel
     */
    public ClientChannelPool(InetSocketAddress address, ChannelInitializer channelInitializer) {
        final Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(address)
                .handler(channelInitializer);

        this.channelPool = new VerySimpleChannelPool(clientBootstrap, new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) throws Exception {
                logger.info("New channel created ({})", ch);
            }

            @Override
            public void channelReleased(Channel ch) throws Exception {
                logger.info("Released channel ({})", ch);
            }
        });
    }

    /**
     * Retrieve IP address.
     *
     * @return  IP address of current network interface
     */
    public String retrieveLocalIp() {
        try {
            final Channel ch = channelPool.acquire().sync().getNow();
            final InetAddress inetAddress = ((InetSocketAddress) ch.localAddress()).getAddress();
            /*first symbol of the host name is `/`*/
            final String local = inetAddress.toString().substring(1);
            this.channelPool.release(ch);
            return local;
        } catch (Exception e) {
            logger.error("Failed to retrieve local address", e);
        }
        return "";
    }

    /**
     * Closes channel pool and frees all resources.
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        this.channelPool.close();
        this.group.shutdownGracefully();
    }

    /**
     * Send request cancellation and frees acquired channel.
     *
     * @param pack  cancellation rpc-style request packed into bus package
     */
    public void sendCancellation(final BusPackage pack) {
        final Future<Channel> future = this.channelPool.acquire();
        future.addListener(futureListener -> {
            if (futureListener.isSuccess()) {
                final Channel ch = (Channel) futureListener.getNow();
                ch.writeAndFlush(pack).addListener(future1 -> channelPool.release(ch));
            }
        });
    }

    /**
     * Sending request to RPC server using channel form channel pool.
     *
     * @param pack  request to RPC server packed into bus package
     * @return      future which completes as soon as the answer received or exception is thrown
     */
    public CompletableFuture<BusPackage> sendRequest(final BusPackage pack) {
        final CompletableFuture<BusPackage> busFuture = new CompletableFuture<>();
        final Future<Channel> future = this.channelPool.acquire();

        future.addListener((Future<Channel> futureListener) -> {
            if (futureListener.isSuccess()) {
                final Channel ch = futureListener.getNow();
                ch.pipeline().get(ChannelPoolHandler.class).setCompletableFuture(busFuture);
                ch.writeAndFlush(pack).addListener(genericFutureListener -> {
                    logger.info("Send request requestId={} with channel {}", pack, ch);
                    if (!genericFutureListener.isSuccess()) {
                        logger.error("Failed to write message ({}) to TCP channel", pack,
                                     genericFutureListener.cause());
                        busFuture.completeExceptionally(genericFutureListener.cause());
                    }
                });
                busFuture.whenComplete((busPackage, throwable) -> this.channelPool.release(ch));
            } else {
                logger.error("Failed to acquire new channel", futureListener.cause());
                busFuture.completeExceptionally(futureListener.cause());
            }
        });
        return busFuture;
    }
}
