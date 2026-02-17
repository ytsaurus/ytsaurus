package tech.ytsaurus.client.rpc;

/**
 * Listener for tracking bytes sent and received by the client.
 */
public interface RpcClientListener {
    /**
     * Called when bytes are sent.
     */
    void onBytesSent(RpcRequestDescriptor context, long bytes);

    /**
     * Called when bytes are received.
     */
    default void onBytesReceived(RpcRequestDescriptor context, long bytes) {
    }
}
