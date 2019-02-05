package ru.yandex.yt.ytclient.rpc.internal;

import java.util.concurrent.ScheduledExecutorService;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcCompression;

public class RpcClientWithCompression implements RpcClient {
    private final RpcClient client;
    private final RpcCompression compression;

    public RpcClientWithCompression(RpcClient client, RpcCompression compression) {
        this.client = client;
        this.compression = compression;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
        TRequestHeader.Builder header = request.header();
        if (!header.hasRequestCodecs()) {
            header.setRequestCodecs(TRequestHeader.TRequestCodecs.newBuilder()
                    .setRequestCodec(compression.getRequestCodecId().getValue())
                    .setResponseCodec(compression.getResponseCodecId().getValue())
                    .setRequestAttachmentCodec(compression.getRequestAttachmentsCodecId().getValue())
                    .setResponseAttachmentCodec(compression.getResponseAttachmentsCodecId().getValue())
                    .build());
        }
        return client.send(this, request, handler);
    }

    @Override
    public String destinationName() {
        return "RpcClientWithCompression@" + client.toString();
    }

    @Override
    public ScheduledExecutorService executor() {
        return client.executor();
    }
}
