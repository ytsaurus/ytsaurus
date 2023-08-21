package tech.ytsaurus.client.rpc;

/**
 * @author aozeritsky
 */
public interface RpcFailoverPolicy {
    boolean onError(Throwable error);

    boolean onTimeout();

    boolean randomizeDcs();
}
