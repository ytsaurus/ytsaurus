package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TRspGetPipelineSpec;
import tech.ytsaurus.ysontree.YTreeNode;

public class GetPipelineSpecResult {
    private final Long version;
    private final YTreeNode spec;

    public GetPipelineSpecResult(TRspGetPipelineSpec rsp) {
        this.version = rsp.getVersion();
        this.spec = RpcUtil.parseByteString(rsp.getSpec());
    }

    public Long getVersion() {
        return version;
    }

    public YTreeNode getSpec() {
        return spec;
    }
}
