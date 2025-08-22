package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TRspGetFlowView;
import tech.ytsaurus.ysontree.YTreeNode;

public class GetFlowViewResult {
    private final YTreeNode flowViewPart;

    public GetFlowViewResult(TRspGetFlowView rsp) {
        this.flowViewPart = RpcUtil.parseByteString(rsp.getFlowViewPart());
    }

    public YTreeNode getFlowViewPart() {
        return flowViewPart;
    }
}
