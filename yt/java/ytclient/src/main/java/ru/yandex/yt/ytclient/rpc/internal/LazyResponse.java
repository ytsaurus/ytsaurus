package ru.yandex.yt.ytclient.rpc.internal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Реализация RpcClientResponse с ленивой десериализацией тела ответа при первом обращении
 */
public class LazyResponse<ResponseType extends MessageLite> implements RpcClientResponse<ResponseType> {
    private final Parser<ResponseType> parser;
    private byte[] bodyData;
    private ResponseType bodyMessage;
    private final List<byte[]> attachments;
    private final RpcClient sender;
    private final Option<Compression> compression;

    public LazyResponse(
            Parser<ResponseType> parser,
            byte[] body,
            List<byte[]> attachments,
            RpcClient sender,
            Option<TResponseHeader> responseHeader)
    {
        this.parser = parser;
        this.bodyData = body;
        this.sender = Objects.requireNonNull(sender);

        if (responseHeader.isPresent() && responseHeader.get().hasCodec()) {
            this.compression = Option.of(Compression.fromValue(responseHeader.get().getCodec()));
        } else {
            this.compression = Option.empty();
        }

        if (compression.isPresent()) {
            Codec codec = Codec.codecFor(compression.get());
            this.attachments = attachments.stream().map(codec::decompress).collect(Collectors.toList());
        } else {
            this.attachments = attachments;
        }
    }

    @Override
    public synchronized ResponseType body() {
        if (bodyData != null) {
            bodyMessage = compression.isPresent()
                    ? RpcUtil.parseMessageBodyWithCompression(bodyData, parser, compression.get())
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
