package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.TRspGetPipelineState;

public class GetPipelineStateResult {
    private final PipelineState state;

    public GetPipelineStateResult(TRspGetPipelineState rsp) {
        this.state = PipelineState.valueOf(rsp.getState());
    }

    public PipelineState getState() {
        return state;
    }
}
