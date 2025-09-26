package tech.ytsaurus.client.rpc;

/**
 * Listener for tracking bytes sent by the client.
 */
public interface RpcClientListener {
    /**
     * Called when bytes are sent.
     */
    void onBytesSent(RpcRequestDescriptor context, long bytes);
}
