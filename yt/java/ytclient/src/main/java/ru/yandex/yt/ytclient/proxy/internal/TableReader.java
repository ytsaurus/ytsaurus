package ru.yandex.yt.ytclient.proxy.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Option;
import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;
import ru.yandex.yt.rpcproxy.TRspReadTable;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientResponse;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcMessageParser;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.LazyResponse;
import ru.yandex.yt.ytclient.rpc.internal.RpcServiceMethodDescriptor;

public class TableReader implements RpcStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TableReader.class);

    private final static RpcMessageParser<TRspReadTable> responseParser = RpcServiceMethodDescriptor.makeMessageParser(TRspReadTable.class);
    private final CompletableFuture<RpcClientResponse<TRspReadTable>> result;

    private final RpcClientStreamControl control;
    private long offset = 0;

    public TableReader(RpcClientStreamControl control) {
        this.control = control;
        this.control.subscribe(this);
        this.control.sendEof();
        this.result = new CompletableFuture<>();
    }

    @Override
    public void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments) {
        // throw new IllegalArgumentException();
        logger.debug("Feedback {} {}", header.getReadPosition(), attachments.size());

        // this.control.sendEof();
    }

    @Override
    public void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments) {

        logger.debug("payload ");

        for (byte[] attachment : attachments) {
            if (attachment != null) {
                offset += attachment.length;
            } else {

                logger.debug("EOF");

                offset += 1;
            }
        }

        logger.debug("offset {} {} ", offset, RpcUtil.fromProto(header.getRequestId()));

        control.feedback(offset);
    }

    @Override
    public void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments) {
        logger.debug("Response");

        if (!result.isDone()) {
            if (attachments.size() < 1 || attachments.get(0) == null) {
                throw new IllegalStateException("Received response without a body");
            }
            result.complete(new LazyResponse<>(responseParser, attachments.get(0),
                    new ArrayList<>(attachments.subList(1, attachments.size())), sender,
                    Option.of(header)));
        }
    }

    @Override
    public void onError(RpcClient sender, Throwable error) {
        logger.error("Error", error);

        if (!result.isDone()) {
            result.completeExceptionally(error);
        }
    }

    public void cancel() {
        control.cancel();
    }

    public void waitResult() throws Exception {
        result.get();
    }
}
