package tech.ytsaurus.client.bus;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.ServerChannel;

public class DefaultBusServerChannel implements BusServer, BusServerLifecycle {
    private final ServerChannel channel;
    private final ChannelPromise bound;

    public DefaultBusServerChannel(ServerChannel channel) {
        this.channel = Objects.requireNonNull(channel);
        this.bound = channel.newPromise();
        this.bound.setUncancellable();
    }

    @Override
    public ServerChannel channel() {
        return channel;
    }

    @Override
    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    @Override
    public ChannelFuture bound() {
        return bound;
    }

    @Override
    public ChannelFuture closed() {
        return channel.closeFuture();
    }

    @Override
    public SocketAddress localAddress() {
        return channel.localAddress();
    }

    @Override
    public CompletableFuture<Void> close() {
        return BusUtil.makeCompletableFuture(channel.close());
    }

    @Override
    public void listenSucceeded() {
        bound.trySuccess();
    }

    @Override
    public void listenFailed(Throwable cause) {
        bound.tryFailure(cause);
    }
}
