package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.ytclient.misc.YtGuid;

public class MutatingOptions {
    private YtGuid id;
    private Boolean retry;

    public MutatingOptions setMutationId(YtGuid id) {
        this.id = id;
        return this;
    }

    public MutatingOptions setRetry(boolean retry) {
        this.retry = retry;
        return this;
    }

    public TMutatingOptions.Builder writeTo(TMutatingOptions.Builder builder) {
        if (id != null) {
            builder.setMutationId(id.toProto());
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
