package ru.yandex.yt.rpc.protocol.proto;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.google.common.primitives.Bytes;

import ru.yandex.yt.TSerializedMessageEnvelope;
import ru.yandex.yt.rpc.protocol.BUSPartable;

/**
 * @author valri
 */
public class ProtoMessageEnvelope implements BUSPartable {
    private final List<Byte> list = new ArrayList<>();

    public ProtoMessageEnvelope(byte[] toWrap) {
        final int totalLength  = toWrap.length + 2 * Integer.BYTES;
        final ByteBuffer bufForList = ByteBuffer.allocate(totalLength);
        bufForList.order(ByteOrder.LITTLE_ENDIAN);
        final TSerializedMessageEnvelope envelope = TSerializedMessageEnvelope
                .newBuilder().build();
        bufForList.putInt(envelope.toByteArray().length);
        bufForList.putInt(toWrap.length);
        bufForList.put(toWrap);
        list.addAll(Bytes.asList(bufForList.array()));
    }

    @Override
    public List<Byte> getBusPart() {
        return this.list;
    }
}
