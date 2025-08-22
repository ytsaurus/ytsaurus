package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.TRspSetPipelineSpec;

public class SetPipelineSpecResult {
    private final Long version;

    public SetPipelineSpecResult(TRspSetPipelineSpec rsp) {
        this.version = rsp.getVersion();
    }

    public Long getVersion() {
        return version;
    }
}
