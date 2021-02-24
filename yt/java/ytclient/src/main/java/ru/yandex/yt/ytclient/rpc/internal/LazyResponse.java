package ru.yandex.yt.ytclient.rpc.internal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Реализация RpcClientResponse с ленивой десериализацией тела ответа при первом обращении
 */
@NonNullApi
@NonNullFields
public class LazyResponse<ResponseType extends MessageLite> implements RpcClientResponse<ResponseType> {
    private final Parser<ResponseType> parser;
    @Nullable
    private byte[] bodyData;
    @Nullable
    private ResponseType bodyMessage;
    private final List<byte[]> attachments;
    private final RpcClient sender;
    @Nullable
    private final Compression compression;

    public LazyResponse(
            Parser<ResponseType> parser,
            byte[] body,
            List<byte[]> attachments,
            RpcClient sender,
            @Nullable TResponseHeader responseHeader)
    {
        this.parser = parser;
        this.bodyData = body;
        this.sender = Objects.requireNonNull(sender);

        if (responseHeader != null && responseHeader.hasCodec()) {
            this.compression = Compression.fromValue(responseHeader.getCodec());
        } else {
            this.compression = null;
        }

        if (compression != null) {
            Codec codec = Codec.codecFor(compression);
            this.attachments = attachments.stream().map(codec::decompress).collect(Collectors.toList());
        } else {
            this.attachments = attachments;
        }
    }

    @Override
    public synchronized ResponseType body() {
        if (bodyMessage == null) {
            bodyMessage = compression != null
                    ? RpcUtil.parseMessageBodyWithCompression(bodyData, parser, compression)
                    : RpcUtil.parseMessageBodyWithEnvelope(bodyData, parser);
            bodyData = null;
        }
        return bodyMessage;
    }

    @Override
    public List<byte[]> attachments() {
        return attachments;
    }

    @Override
    public RpcClient sender() {
        return sender;
    }
}
