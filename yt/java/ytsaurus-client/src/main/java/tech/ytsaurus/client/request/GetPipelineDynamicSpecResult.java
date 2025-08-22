package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TRspGetPipelineDynamicSpec;
import tech.ytsaurus.ysontree.YTreeNode;

public class GetPipelineDynamicSpecResult {
    private final Long version;
    private final YTreeNode spec;

    public GetPipelineDynamicSpecResult(TRspGetPipelineDynamicSpec rsp) {
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
