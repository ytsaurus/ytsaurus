package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TRspFlowExecute;
import tech.ytsaurus.ysontree.YTreeNode;

public class FlowExecuteResult {
    private final YTreeNode result;

    public FlowExecuteResult(TRspFlowExecute rsp) {
        this.result = RpcUtil.parseByteString(rsp.getResult());
    }

    public YTreeNode getResult() {
        return result;
    }
}
