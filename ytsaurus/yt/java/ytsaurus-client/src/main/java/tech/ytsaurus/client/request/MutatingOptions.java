package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.TMutatingOptions;

public class MutatingOptions {
    private GUID id;
    private Boolean retry;

    public MutatingOptions() {
    }

    public MutatingOptions(MutatingOptions mutatingOptions) {
        id = mutatingOptions.id;
        retry = mutatingOptions.retry;
    }

    public MutatingOptions setMutationId(GUID id) {
        this.id = id;
        return this;
    }

    public MutatingOptions setRetry(boolean retry) {
        this.retry = retry;
        return this;
    }

    public GUID getMutationId() {
        return this.id;
    }

    public TMutatingOptions.Builder writeTo(TMutatingOptions.Builder builder) {
        if (id != null) {
            builder.setMutationId(RpcUtil.toProto(id));
        }
        if (retry != null) {
            builder.setRetry(retry);
        }
        return builder;
    }

    public TMutatingOptions toProto() {
        return writeTo(TMutatingOptions.newBuilder()).build();
    }
}
