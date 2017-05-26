package ru.yandex.yt.ytclient.rpc.internal;

import java.util.List;

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

    public LazyResponse(RpcMessageParser<ResponseType> parser, byte[] body, List<byte[]> attachments) {
        this.parser = parser;
        this.bodyData = body;
        this.attachments = attachments;
    }

    @Override
    public synchronized ResponseType body() {
        if (bodyData != null) {
            bodyMessage = RpcUtil.parseMessageBodyWithEnvelope(bodyData, parser);
            bodyData = null;
        }
        return bodyMessage;
    }

    @Override
    public List<byte[]> attachments() {
        return attachments;
    }
}
