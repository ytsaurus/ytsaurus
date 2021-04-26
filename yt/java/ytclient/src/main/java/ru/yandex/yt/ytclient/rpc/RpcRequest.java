package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.List;

import com.google.protobuf.MessageLite;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.TRequestHeaderOrBuilder;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class RpcRequest<RequestType extends MessageLite> {
    public final TRequestHeader header;
    public final RequestType body;
    public final List<byte[]> attachments;

    public RpcRequest(TRequestHeader header, RequestType body, List<byte[]> attachments) {
        this.header = header;
        this.body = body;
        this.attachments = attachments;
    }

    public static Duration getTimeout(TRequestHeaderOrBuilder header) {
        if (header.hasTimeout()) {
            return RpcUtil.durationFromMicros(header.getTimeout());
        } else {
            return null;
        }
    }

    public static GUID getRequestId(TRequestHeaderOrBuilder header) {
        return RpcUtil.fromProto(header.getRequestId());
    }

    static List<byte[]> serialize(TRequestHeader header, MessageLite body, List<byte[]> attachments) {
        return RpcUtil.createRequestMessage(header, body, attachments);
    }

    @Override
    public String toString() {
        return String.format("%s (RequestId: %s)",
                header.getMethod(), getRequestId(header));
    }
}
