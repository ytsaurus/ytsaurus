package ru.yandex.yt.ytclient.proxy.request;

import java.util.List;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

public class ConcatenateNodes extends MutateNode<ConcatenateNodes> {
    private final String [] from;
    private final String to;

    public ConcatenateNodes(String [] from, String to) {
        this.from = from;
        this.to = to;
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        this((String[])source.stream().map(YPath::toString).toArray(), dest.toString());
    }

    public TReqConcatenateNodes.Builder writeTo(TReqConcatenateNodes.Builder builder) {
        for (int i = 0; i < from.length; ++i) {
            builder.addSrcPaths(from[i]);
        }

        builder.setDstPath(to);

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

    @Nonnull
    @Override
    protected ConcatenateNodes self() {
        return this;
    }
}
