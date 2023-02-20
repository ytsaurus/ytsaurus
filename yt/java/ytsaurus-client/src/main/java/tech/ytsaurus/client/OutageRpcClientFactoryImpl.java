package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcClientWrapper;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

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

    private RpcClientResponseHandler wrapHandler(RpcClientResponseHandler handler, String method, GUID requestId) {
        return new RpcClientResponseHandler() {
            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = Objects.requireNonNull(controller.pollError(method, requestId));
                if (error.isEmpty()) {
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

    private RpcStreamConsumer wrapConsumer(RpcStreamConsumer consumer, String method, GUID requestId) {
        return new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                consumer.onStartStream(control);
            }

            @Override
            public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = Objects.requireNonNull(controller.pollError(method, requestId));
                if (error.isEmpty()) {
                    consumer.onFeedback(sender, header, attachments);
                    return;
                }

                consumer.onError(error.get());
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                Optional<Throwable> error = Objects.requireNonNull(controller.pollError(method, requestId));
                if (error.isEmpty()) {
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
                Optional<Throwable> error = Objects.requireNonNull(controller.pollError(method, requestId));
                if (error.isEmpty()) {
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
        String method = request.header.getMethod();
        Optional<Duration> delay = controller.pollDelay(method);
        if (delay.isPresent()) {
            ScheduledFuture<RpcClientRequestControl> task = executor().schedule(
                    () -> super.send(
                            sender,
                            request,
                            wrapHandler(handler, method, RpcUtil.fromProto(request.header.getRequestId())),
                            options
                    ),
                    delay.get().toNanos(), TimeUnit.NANOSECONDS);
            return new OutageRpcClientRequestControl(task);
        }
        return super.send(
                sender,
                request,
                wrapHandler(handler, method, RpcUtil.fromProto(request.header.getRequestId())),
                options
        );
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        return super.startStream(
                sender,
                request,
                wrapConsumer(consumer, request.header.getMethod(), RpcUtil.fromProto(request.header.getRequestId())),
                options
        );
    }
}

@NonNullFields
@NonNullApi
class OutageRpcClientRequestControl implements RpcClientRequestControl {
    final ScheduledFuture<RpcClientRequestControl> task;

    OutageRpcClientRequestControl(ScheduledFuture<RpcClientRequestControl> task) {
        this.task = task;
    }

    @Override
    public boolean cancel() {
        boolean result = false;
        if (!task.isDone()) {
            result = task.cancel(false);
        }
        if (result) {
            return true;
        }
        try {
            return task.get().cancel();
        } catch (ExecutionException | InterruptedException ex) {
            return false;
        } catch (CancellationException ex) {
            return true;
        }
    }
}

@NonNullFields
@NonNullApi
class OutageRpcClientFactoryImpl extends RpcClientFactoryImpl {
    final OutageController controller;

    OutageRpcClientFactoryImpl(
            BusConnector connector,
            YTsaurusClientAuth auth,
            RpcCompression compression,
            OutageController controller
    ) {
        super(connector, auth, compression);
        this.controller = controller;
    }

    @Override
    public RpcClient create(HostPort hostPort, String name) {
        return new OutageRpcClient(super.create(hostPort, name), controller);
    }
}
