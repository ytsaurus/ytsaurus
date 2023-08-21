package tech.ytsaurus.client.bus;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Deque;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.GUID;

class BusProtocolHandler extends ChannelDuplexHandler {
    private static final Logger logger = LoggerFactory.getLogger(BusProtocolHandler.class);

    private final Bus bus;
    private final BusListenerWrapper wrappedListener;
    private final Deque<DeliveryEntry> deliveryQueue = new ArrayDeque<>();

    private static class DeliveryEntry {
        private final GUID packetId;
        private final ChannelPromise promise;

        DeliveryEntry(GUID packetId, ChannelPromise promise) {
            this.packetId = packetId;
            this.promise = promise;
        }

        public GUID getPacketId() {
            return packetId;
        }

        public ChannelPromise getPromise() {
            return promise;
        }
    }

    BusProtocolHandler(Bus bus, BusListener listener) {
        this.bus = bus;
        this.wrappedListener = new BusListenerWrapper(listener);
    }

    private void abortDelivery(Throwable cause) {
        DeliveryEntry entry;
        while ((entry = deliveryQueue.pollFirst()) != null) {
            entry.getPromise().tryFailure(cause);
        }
    }

    /**
     * Возвращает bus, ассоциированный с данным обработчиком
     */
    public Bus getBus() {
        return bus;
    }

    /**
     * Возвращает listener, ассоциированный с данным обработчиком
     */
    public BusListener getListener() {
        return wrappedListener.getListener();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.debug("Unhandled exception", cause);
        try {
            try {
                try {
                    abortDelivery(cause);
                } finally {
                    wrappedListener.onException(bus, cause);
                }
            } finally {
                if (bus instanceof BusLifecycle) {
                    ((BusLifecycle) bus).channelFailed(cause);
                }
            }
        } finally {
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        if (bus instanceof BusLifecycle) {
            ((BusLifecycle) bus).channelConnected();
        }
        wrappedListener.onConnect(bus);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            try {
                abortDelivery(new ClosedChannelException());
            } finally {
                wrappedListener.onDisconnect(bus);
            }
        } finally {
            if (bus instanceof BusLifecycle) {
                ((BusLifecycle) bus).channelDisconnected();
            }
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BusPacket) {
            BusPacket packet = (BusPacket) msg;
            switch (packet.getType()) {
                case ACK:
                    DeliveryEntry entry = deliveryQueue.peekFirst();
                    if (entry == null) {
                        ctx.close();
                        throw new IllegalStateException(
                                "Received unexpected ack for packet " + packet.getPacketId());
                    }
                    if (!entry.getPacketId().equals(packet.getPacketId())) {
                        ctx.close();
                        throw new IllegalStateException("Received unexpected ack for packet " + packet.getPacketId()
                                + " while waiting for packet " + entry.getPacketId());
                    }
                    deliveryQueue.removeFirst();
                    entry.getPromise().trySuccess();
                    break;
                case MESSAGE:
                    if (packet.hasFlags(BusPacketFlags.REQUEST_ACK)) {
                        ctx.writeAndFlush(
                                new BusPacket(BusPacketType.ACK, BusPacketFlags.NONE, packet.getPacketId()));
                    }
                    wrappedListener.onMessage(bus, packet.getMessage());
                    break;
                default:
                    ctx.close();
                    throw new IllegalStateException("Unexpected packet received: " + packet.getType());
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof BusOutgoingMessage) {
            BusOutgoingMessage outgoingMessage = (BusOutgoingMessage) msg;
            if (promise.isDone()) {
                // Если на promise вызывали cancel, то даже не пытаемся отсылать пакет
                return;
            }
            if (!ctx.channel().isActive()) {
                // Если канал сейчас не активен, то даже не пытаемся отсылать пакет
                promise.tryFailure(new ClosedChannelException());
                return;
            }
            GUID packetId = outgoingMessage.getPacketId();
            short flags = BusPacketFlags.NONE;
            BusDeliveryTracking level = outgoingMessage.getLevel();
            ChannelPromise writePromise = promise;
            switch (level) {
                case NONE:
                    // Создаём новый promise на запись
                    writePromise = ctx.newPromise();
                    break;
                case FULL:
                    // Добавляем пакет в очередь на подтверждение
                    writePromise = ctx.newPromise();
                    flags |= BusPacketFlags.REQUEST_ACK;
                    deliveryQueue.add(new DeliveryEntry(packetId, promise));
                    break;
                default:
                    break;
            }
            // Формируем пакет с сообщением и пишем его в контекст
            BusPacket packet = new BusPacket(BusPacketType.MESSAGE, flags, packetId, outgoingMessage.getMessage());
            ctx.write(packet, writePromise);
            if (level == BusDeliveryTracking.NONE) {
                // На уровне NONE завершаем promise не дожидаясь записи
                promise.trySuccess();
            }
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
