package tech.ytsaurus.client.rpc;

/**
 * @author aozeritsky
 */
public class AlwaysSwitchRpcFailoverPolicy implements RpcFailoverPolicy {
    @Override
    public boolean onError(Throwable error) {
        return true;
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
