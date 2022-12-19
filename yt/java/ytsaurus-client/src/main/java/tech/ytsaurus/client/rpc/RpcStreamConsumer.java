package tech.ytsaurus.client.rpc;

import java.util.List;

import tech.ytsaurus.rpc.TStreamingFeedbackHeader;
import tech.ytsaurus.rpc.TStreamingPayloadHeader;

public interface RpcStreamConsumer extends RpcClientResponseHandler {
    void onStartStream(RpcClientStreamControl control);

    void onFeedback(RpcClient sender, TStreamingFeedbackHeader header, List<byte[]> attachments);

    void onPayload(RpcClient sender, TStreamingPayloadHeader header, List<byte[]> attachments);

    void onWakeup();
}
