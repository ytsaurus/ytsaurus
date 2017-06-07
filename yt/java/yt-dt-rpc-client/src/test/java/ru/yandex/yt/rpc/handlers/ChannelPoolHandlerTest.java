package ru.yandex.yt.rpc.handlers;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.yt.rpc.protocol.bus.BusPackage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author valri
 */
public class ChannelPoolHandlerTest {
    final CompletableFuture<BusPackage> cf = new CompletableFuture<>();
    final ChannelPoolHandler handler = new ChannelPoolHandler();
    EmbeddedChannel ch;
    UUID uuid;

    @Before
    public void init() {
        handler.setCompletableFuture(cf);
        ch = new EmbeddedChannel(handler);
        uuid = UUID.randomUUID();
    }

    @Test
    public void channelReadOkMessage() throws Exception {
        BusPackage request = new BusPackage(BusPackage.PacketType.MESSAGE, BusPackage.PacketFlags.NONE,
                uuid, false, new ArrayList<>());
        ch.writeInbound(request);
        assertTrue(cf.isDone());
    }

    @Test
    public void channelReadOkMessagePlusACK() throws Exception {
        BusPackage request = new BusPackage(BusPackage.PacketType.MESSAGE, BusPackage.PacketFlags.REQUEST_ACK,
                uuid, false, new ArrayList<>());
        ch.writeInbound(request);
        assertTrue(cf.isDone());
    }

    @Test
    public void channelReadACKMessage() throws Exception {
        BusPackage request = new BusPackage(BusPackage.PacketType.ACK, BusPackage.PacketFlags.NONE,
                uuid, false, new ArrayList<>());
        ch.writeInbound(request);
        assertFalse(cf.isDone());
    }
}
