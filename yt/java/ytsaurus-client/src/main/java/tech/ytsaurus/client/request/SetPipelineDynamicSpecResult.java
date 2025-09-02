package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.TRspSetPipelineDynamicSpec;

public class SetPipelineDynamicSpecResult {
    private final Long version;

    public SetPipelineDynamicSpecResult(TRspSetPipelineDynamicSpec rsp) {
        this.version = rsp.getVersion();
    }

    public Long getVersion() {
        return version;
    }
}
