package ru.yandex.yt.ytclient.rpc;

import java.util.List;

import ru.yandex.yt.rpc.TStreamingFeedbackHeader;
import ru.yandex.yt.rpc.TStreamingPayloadHeader;

public interface RpcStreamConsumer extends RpcClientResponseHandler {
    void onStartStream(RpcClientStreamControl control);
    void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments);
    void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments);
    void onWakeup();
}
