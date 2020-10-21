package ru.yandex.yt.ytclient.rpc.internal;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcClientWrapper;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

public class RpcClientWithCompression extends RpcClientWrapper {
    private final RpcCompression compression;

    public RpcClientWithCompression(RpcClient client, RpcCompression compression) {
        super(client);
        this.compression = compression;
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
        return super.send(sender, request, handler);
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request, RpcStreamConsumer consumer) {
        patchHeader(request);
        return super.startStream(sender, request, consumer);
    }

    @Override
    public String toString() {
        final Compression in = compression.getRequestCodecId();
        final Compression out = compression.getResponseCodecId();
        if (in == out) {
            if (in == Compression.None) {
                return super.toString();
            } else {
                return in + "@" + super.toString();
            }
        } else {
            return in + "/" + out + "@" + super.toString();
        }
    }
}
