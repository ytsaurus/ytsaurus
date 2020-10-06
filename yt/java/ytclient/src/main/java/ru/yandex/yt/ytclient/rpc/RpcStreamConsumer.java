package ru.yandex.yt.ytclient.rpc;

import java.util.List;
import java.util.concurrent.CancellationException;

import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;

public interface RpcStreamConsumer {
    void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments);
    void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments);
    void onResponse(RpcClient sender, TResponseHeader header, List<byte[]> attachments);
    void onError(RpcClient sender, Throwable cause);
    void onCancel(RpcClient sender, CancellationException cancel);
    default void onWakeup() { }
}
