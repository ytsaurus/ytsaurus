package tech.ytsaurus.client.rpc;

/**
 * @author aozeritsky
 */
public class DefaultRpcFailoverPolicy implements RpcFailoverPolicy {
    @Override
    public boolean onError(Throwable error) {
        return false;
    }

    @Override
    public boolean onTimeout() {
        return true;
    }

    @Override
    public boolean randomizeDcs() {
        return false;
    }
}
