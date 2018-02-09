package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class ConcatenateNodes extends MutateNode<ConcatenateNodes> {
    private final String [] from;
    private final String to;
    private boolean append = false;

    public ConcatenateNodes(String [] from, String to) {
        this.from = from;
        this.to = to;
    }

    public ConcatenateNodes setAppend(boolean append) {
        this.append = append;
        return this;
    }

    public TReqConcatenateNodes.Builder writeTo(TReqConcatenateNodes.Builder builder) {
        for (int i = 0; i < from.length; ++i) {
            builder.addSrcPaths(from[i]);
        }

        builder.setDstPath(to)
                .setAppend(append);

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }

        return builder;
    }
}
