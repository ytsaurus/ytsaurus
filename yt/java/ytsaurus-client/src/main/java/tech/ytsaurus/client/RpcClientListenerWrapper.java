package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientListener;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcClientWrapper;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.client.rpc.RpcRequestDescriptor;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.rpc.TResponseHeader;
import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

/**
 * RpcClient wrapper that notifies {@link RpcClientListener} about bytes sent and received.
 */
class RpcClientListenerWrapper extends RpcClientWrapper {
    private final RpcClientListener listener;

    RpcClientListenerWrapper(RpcClient delegate, RpcClientListener listener) {
        super(delegate);
        this.listener = listener;
    }

    @Override
    public RpcClientRequestControl send(RpcClient sender, RpcRequest<?> request, RpcClientResponseHandler handler,
                                        RpcOptions options) {
        RpcRequestDescriptor context = createContext(request, false);
        return super.send(
                sender,
                ListeningRpcRequest.wrap(request, listener, context),
                ListeningResponseHandler.wrap(handler, listener, context),
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
        RpcRequestDescriptor requestContext = createContext(request, false);
        RpcRequestDescriptor streamContext = createContext(request, true);
        RpcStreamConsumer meteredConsumer = new RpcStreamConsumer() {
            @Override
            public void onStartStream(RpcClientStreamControl control) {
                RpcClientStreamControl wrapped = new MeteredStreamControl(control, listener, streamContext);
                consumer.onStartStream(wrapped);
            }

            @Override
            public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
                consumer.onFeedback(sender, header, attachments);
            }

            @Override
            public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {
                long size = attachments.stream().mapToInt(RpcUtil::attachmentSize).sum();
                if (size > 0) {
                    listener.onBytesReceived(streamContext, size);
                }
                consumer.onPayload(sender, header, attachments);
            }

            @Override
            public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
                long size = attachments.stream().mapToInt(RpcUtil::attachmentSize).sum();
                if (size > 0) {
                    listener.onBytesReceived(requestContext, size);
                }
                consumer.onResponse(sender, header, attachments);
            }

            @Override
            public void onError(Throwable cause) {
                consumer.onError(cause);
            }

            @Override
            public void onCancel(CancellationException cancel) {
                consumer.onCancel(cancel);
            }

            @Override
            public void onWakeup() {
                consumer.onWakeup();
            }
        };

        return super.startStream(
                sender,
                ListeningRpcRequest.wrap(request, listener, requestContext),
                meteredConsumer,
                options
        );
    }

    private static RpcRequestDescriptor createContext(RpcRequest<?> request, boolean isStream) {
        RpcRequestDescriptor.Builder builder = RpcRequestDescriptor.builder()
                .setService(request.header.getService())
                .setMethod(request.header.getMethod())
                .setRequestId(RpcRequest.getRequestId(request.header));
        if (isStream) {
            builder.setIsStream(true);
        }
        return builder.build();
    }
}

class ListeningResponseHandler implements RpcClientResponseHandler {
    private final RpcClientResponseHandler inner;
    private final RpcClientListener listener;
    private final RpcRequestDescriptor context;

    private ListeningResponseHandler(
            RpcClientResponseHandler inner,
            RpcClientListener listener,
            RpcRequestDescriptor context
    ) {
        this.inner = inner;
        this.listener = listener;
        this.context = context;
    }

    static RpcClientResponseHandler wrap(
            RpcClientResponseHandler handler,
            RpcClientListener listener,
            RpcRequestDescriptor context
    ) {
        return new ListeningResponseHandler(handler, listener, context);
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        long size = attachments.stream().mapToInt(RpcUtil::attachmentSize).sum();
        if (size > 0) {
            listener.onBytesReceived(context, size);
        }
        inner.onResponse(sender, header, attachments);
    }

    @Override
    public void onError(Throwable error) {
        inner.onError(error);
    }

    @Override
    public void onCancel(CancellationException cancel) {
        inner.onCancel(cancel);
    }
}

class ListeningRpcRequest<RequestType extends MessageLite> extends RpcRequest<RequestType> {
    private final RpcClientListener listener;
    private final RpcRequestDescriptor context;

    private ListeningRpcRequest(
            RpcRequest<RequestType> rpcRequest,
            RpcClientListener listener,
            RpcRequestDescriptor context) {
        super(rpcRequest);
        this.listener = listener;
        this.context = context;
    }

    static RpcRequest<?> wrap(RpcRequest<?> original, RpcClientListener listener, RpcRequestDescriptor context) {
        return new ListeningRpcRequest<>(original, listener, context);
    }

    @Override
    public RpcRequest<RequestType> copy(TRequestHeader header) {
        return new ListeningRpcRequest<>(super.copy(header), listener, context);
    }

    @Override
    protected List<byte[]> serialize(TRequestHeader header) {
        List<byte[]> serialized = super.serialize(header);
        long sum = serialized.stream().mapToInt(arr -> arr.length).sum();
        if (sum > 0) {
            listener.onBytesSent(context, sum);
        }
        return serialized;
    }
}

class MeteredStreamControl implements RpcClientStreamControl {
    private final RpcClientStreamControl inner;
    private final RpcClientListener listener;
    private final RpcRequestDescriptor context;

    MeteredStreamControl(
            RpcClientStreamControl inner,
            RpcClientListener listener,
            RpcRequestDescriptor context
    ) {
        this.inner = inner;
        this.listener = listener;
        this.context = context;
    }

    @Override
    public boolean cancel() {
        return inner.cancel();
    }

    @Override
    public Compression getExpectedPayloadCompression() {
        return inner.getExpectedPayloadCompression();
    }

    @Override
    public CompletableFuture<Void> feedback(long offset) {
        return inner.feedback(offset);
    }

    @Override
    public CompletableFuture<Void> sendEof() {
        return inner.sendEof();
    }

    @Override
    public CompletableFuture<Void> sendPayload(List<byte[]> attachments) {
        CompletableFuture<Void> future = inner.sendPayload(attachments);
        long attachmentsSize = attachments.stream().mapToInt(RpcUtil::attachmentSize).sum();
        if (attachmentsSize > 0) {
            listener.onBytesSent(context, attachmentsSize);
        }
        return future;
    }

    @Override
    public void wakeUp() {
        inner.wakeUp();
    }

    @Override
    public String getRpcProxyAddress() {
        return inner.getRpcProxyAddress();
    }
}


