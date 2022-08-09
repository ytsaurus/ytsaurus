package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class ConcatenateNodes
        extends MutateNode<ConcatenateNodes>
        implements HighLevelRequest<TReqConcatenateNodes.Builder> {
    private final List<YPath> srcPaths;
    private final YPath dstPath;

    public ConcatenateNodes(String[] from, String to) {
        this(
                Arrays.stream(from).map(YPath::simple).collect(Collectors.toList()),
                YPath.simple(to)
        );
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        this.srcPaths = source;
        this.dstPath = dest;
    }

    public List<YPath> getSourcePaths() {
        return srcPaths;
    }

    public YPath getDestinationPath() {
        return dstPath;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqConcatenateNodes.Builder, ?> requestBuilder) {
        TReqConcatenateNodes.Builder builder = requestBuilder.body();
        for (YPath s : srcPaths) {
            builder.addSrcPaths(s.toString());
        }

        builder.setDstPath(dstPath.toString());

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb
                .append("SourcePaths: ")
                .append(Arrays.toString(srcPaths.toArray()))
                .append("; DstPath: ")
                .append(dstPath.toString())
                .append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("source_paths").value(srcPaths, (b2, t) -> t.toTree(b2))
                .key("destination_path").apply(dstPath::toTree);
    }

    @Nonnull
    @Override
    protected ConcatenateNodes self() {
        return this;
    }
}
