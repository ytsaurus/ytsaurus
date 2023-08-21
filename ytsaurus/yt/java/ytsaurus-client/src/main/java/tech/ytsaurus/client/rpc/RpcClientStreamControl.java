package tech.ytsaurus.client.rpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface RpcClientStreamControl extends RpcClientRequestControl {
    Compression getExpectedPayloadCompression();

    CompletableFuture<Void> feedback(long offset);

    CompletableFuture<Void> sendEof();

    CompletableFuture<Void> sendPayload(List<byte[]> attachments);

    void wakeUp();

    String getRpcProxyAddress();
}
