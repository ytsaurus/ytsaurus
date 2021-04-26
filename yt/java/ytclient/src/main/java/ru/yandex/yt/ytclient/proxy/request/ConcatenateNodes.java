package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public class ConcatenateNodes
        extends MutateNode<ConcatenateNodes>
        implements HighLevelRequest<TReqConcatenateNodes.Builder> {
    private final String[] srcPaths;
    private final String dstPath;

    public ConcatenateNodes(String[] from, String to) {
        this.srcPaths = from;
        this.dstPath = to;
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        this((String[]) source.stream().map(YPath::toString).toArray(), dest.toString());
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqConcatenateNodes.Builder, ?> requestBuilder) {
        TReqConcatenateNodes.Builder builder = requestBuilder.body();
        for (String s : srcPaths) {
            builder.addSrcPaths(s);
        }

        builder.setDstPath(dstPath);

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (mutatingOptions != null) {
            builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("SourcePaths: ").append(Arrays.toString(srcPaths)).append("; DstPath: ").append(dstPath).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Nonnull
    @Override
    protected ConcatenateNodes self() {
        return this;
    }
}
