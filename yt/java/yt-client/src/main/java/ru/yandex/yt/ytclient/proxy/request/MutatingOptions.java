package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

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
