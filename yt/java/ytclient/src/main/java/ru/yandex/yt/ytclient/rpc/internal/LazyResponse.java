package ru.yandex.yt.ytclient.rpc.internal;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

/**
 * Реализация RpcClientResponse с ленивой десериализацией тела ответа при первом обращении
 */
public class LazyResponse<ResponseType> implements RpcClientResponse<ResponseType> {
    private final RpcMessageParser<ResponseType> parser;
    private byte[] bodyData;
    private ResponseType bodyMessage;
    private final List<byte[]> attachments;
    private final RpcClient sender;
    private final Option<TResponseHeader.TResponseCodecs> codecs;

    public LazyResponse(
            RpcMessageParser<ResponseType> parser,
            byte[] body,
            List<byte[]> attachments,
            RpcClient sender,
            Option<TResponseHeader.TResponseCodecs> codecs)
    {
        this.parser = parser;
        this.bodyData = body;
        this.sender = Objects.requireNonNull(sender);
        this.codecs = Objects.requireNonNull(codecs);

        if (codecs.isPresent()) {
            Codec codec = Codec.codecFor(Compression.fromValue(codecs.get().getResponseAttachmentCodec()));
            this.attachments = attachments.stream().map(codec::decompress).collect(Collectors.toList());
        } else {
            this.attachments = attachments;
        }
    }

    @Override
    public synchronized ResponseType body() {
        if (bodyData != null) {
            bodyMessage = codecs.isPresent()
                    ? RpcUtil.parseMessageBodyWithCompression(bodyData, parser, codecs.get())
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
