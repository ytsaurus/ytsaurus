package ru.yandex.yt.ytclient.rpc.internal;

import java.util.concurrent.ScheduledExecutorService;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

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

    private void patchHeader(RpcClientRequest request) {
        TRequestHeader.Builder header = request.header();
        if (!header.hasRequestCodec()) {
            header
                    .setRequestCodec(compression.getRequestCodecId().getValue())
                    .setResponseCodec(compression.getResponseCodecId().getValue());
        }
    }

    @Override
    public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
        patchHeader(request);
        return client.send(sender, request, handler);
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer) {
        patchHeader(request);
        return client.startStream(sender, request, consumer);
    }

    @Override
    public String destinationName() {
        return client.destinationName();
    }

    @Override
    public ScheduledExecutorService executor() {
        return client.executor();
    }

    @Override
    public String toString() {
        final Compression in = compression.getRequestCodecId();
        final Compression out = compression.getResponseCodecId();
        if (in == out) {
            if (in == Compression.None) {
                return client.toString();
            } else {
                return in + "@" + client.toString();
            }
        } else {
            return in + "/" + out + "@" + client.toString();
        }
    }
}
