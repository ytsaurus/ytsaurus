package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.internal.HostPort;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcClientWrapper;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;


@NonNullApi
@NonNullFields
class OutageRpcClient extends RpcClientWrapper {
    final OutageController controller;

    OutageRpcClient(
            RpcClient innerClient,
            OutageController controller
    ) {
        super(innerClient);
        this.controller = controller;
    }

    private RpcClientResponseHandler wrapHandler(RpcClientResponseHandler handler, String method) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = controller.pollError(method);
                if (!error.isPresent()) {
                    handler.onResponse(sender, header, attachments);
                    return;
                }

                handler.onError(error.get());
            }

            @Override
            public void onError(Throwable error) {
                handler.onError(error);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                handler.onCancel(cancel);
            }
        };
    }

    private RpcStreamConsumer wrapConsumer(RpcStreamConsumer consumer, String method) {
        return new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                consumer.onStartStream(control);
            }

            @Override
            public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = controller.pollError(method);
                if (!error.isPresent()) {
                    consumer.onFeedback(sender, header, attachments);
                    return;
                }

                consumer.onError(error.get());
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = controller.pollError(method);
                if (!error.isPresent()) {
                    consumer.onPayload(sender, header, attachments);
                    return;
                }

                consumer.onError(error.get());
            }

            @Override
            public void onWakeup() {
                consumer.onWakeup();
            }

            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = controller.pollError(method);
                if (!error.isPresent()) {
                    consumer.onResponse(sender, header, attachments);
                    return;
                }

                consumer.onError(error.get());
            }

            @Override
            public void onError(Throwable error) {
                consumer.onError(error);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                consumer.onCancel(cancel);
            }
        };
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        return super.send(sender, request, wrapHandler(handler, request.header.getMethod()), options);
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        return super.startStream(sender, request, wrapConsumer(consumer, request.header.getMethod()), options);
    }
}

@NonNullFields
@NonNullApi
class OutageRpcClientFactoryImpl extends RpcClientFactoryImpl {
    final OutageController controller;

    OutageRpcClientFactoryImpl(
            BusConnector connector,
            RpcCredentials credentials,
            RpcCompression compression,
            OutageController controller
    ) {
        super(connector, credentials, compression);
        this.controller = controller;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        return new OutageRpcClient(super.create(hostPort, name), controller);
    }
}
