package ru.yandex.yt.rpc.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.primitives.Bytes;
import protocol.Guid;
import protocol.Rpc;

import ru.yandex.yt.rpc.protocol.proto.ProtoMessageEnvelope;
import ru.yandex.yt.rpc.protocol.rpc.RpcMessageType;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqHeader;

/**
 * @author valri
 */
public abstract class RpcRequestMessage {
    protected static String serviceName;
    protected static String methodName;

    protected UUID requestId;
    protected RpcReqHeader header;
    protected Rpc.TRequestCancelationHeader cancellationHeader;

    public List<List<Byte>> getBusEnvelope() {
        final List<List<Byte>> fullRequest = new ArrayList<>();
        fullRequest.add(this.header.getBusPart());
        final ProtoMessageEnvelope protoMessageEnvelope = new ProtoMessageEnvelope(this.getRequestBytes());
        fullRequest.add(protoMessageEnvelope.getBusPart());
        return fullRequest;
    }

    public List<List<Byte>> getCancellationBusEnvelope() {
        final Guid.TGuid.Builder uid = Guid.TGuid.newBuilder();
        uid.setFirst(this.requestId.getMostSignificantBits());
        uid.setSecond(this.requestId.getLeastSignificantBits());

        final Rpc.TRequestCancelationHeader.Builder headerBuilder = Rpc.TRequestCancelationHeader.newBuilder();
        headerBuilder.setRequestId(uid.build());
        headerBuilder.setMethod(methodName);
        headerBuilder.setService(serviceName);
        this.cancellationHeader = headerBuilder.build();

        final List<Byte> headerList = new ArrayList<>();
        headerList.addAll(Bytes.asList(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN).putInt(RpcMessageType.REQUEST_CANCELLATION.getValue()).array()));
        headerList.addAll(Bytes.asList(cancellationHeader.toByteArray()));
        final List<List<Byte>> fullRequest = new ArrayList<>();
        fullRequest.add(headerList);
        return fullRequest;
    }


    public UUID getRequestId() {
        return this.requestId;
    }

    protected abstract byte[] getRequestBytes();

    @Override
    public String toString() {
        return MessageFormat.format("{0}->{1}, requestId={2}", serviceName, methodName, requestId);
    }
}
