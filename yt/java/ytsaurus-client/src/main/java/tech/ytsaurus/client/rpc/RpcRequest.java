package tech.ytsaurus.client.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpc.TRequestHeaderOrBuilder;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class RpcRequest<RequestType extends MessageLite> {
    public final TRequestHeader header;
    public final RequestType body;
    public final @Nullable
    List<byte[]> attachments;
    public final @Nullable
    List<byte[]> compressedAttachments;
    public final @Nullable
    Compression compressedAttachmentsCodec;

    public RpcRequest(TRequestHeader header, RequestType body, @Nonnull List<byte[]> attachments) {
        this.header = header;
        this.body = body;
        this.attachments = attachments;
        this.compressedAttachments = null;
        this.compressedAttachmentsCodec = null;
    }

    public RpcRequest(
            TRequestHeader header,
            RequestType body,
            @Nullable Compression codec,
            @Nullable List<byte[]> attachments
    ) {
        this.header = header;
        this.body = body;
        this.attachments = null;
        this.compressedAttachmentsCodec = codec;
        this.compressedAttachments = attachments;
    }

    public RpcRequest<RequestType> copy(TRequestHeader header) {
        if (attachments != null) {
            return new RpcRequest<>(header, body, attachments);
        } else {
            return new RpcRequest<>(header, body, compressedAttachmentsCodec, compressedAttachments);
        }
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

    List<byte[]> serialize(TRequestHeader header) {
        Compression attachmentsCodec = header.hasRequestCodec() ?
                Compression.fromValue(header.getRequestCodec()) :
                Compression.fromValue(0);
        List<byte[]> message = new ArrayList<>(
                2 + Math.max(
                        attachments != null ? attachments.size() : 0,
                        compressedAttachments != null ? compressedAttachments.size() : 0));
        message.add(RpcUtil.createMessageHeader(RpcMessageType.REQUEST, header));
        message.add(header.hasRequestCodec()
                ? RpcUtil.createMessageBodyWithCompression(body, Compression.fromValue(header.getRequestCodec()))
                : RpcUtil.createMessageBodyWithEnvelope(body));
        if (compressedAttachments != null) {
            if (compressedAttachmentsCodec != attachmentsCodec) {
                throw new IllegalArgumentException(
                        "Rpc compression codec doesn't match precomressed attachments compression codec"
                );
            }
            message.addAll(compressedAttachments);
        } else if (attachments != null) {
            message.addAll(RpcUtil.createCompressedAttachments(attachments, attachmentsCodec));
        }
        return message;
    }

    @Override
    public String toString() {
        return String.format("%s (RequestId: %s)",
                header.getMethod(), getRequestId(header));
    }
}
