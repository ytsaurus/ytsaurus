package ru.yandex.yt.ytclient.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqConcatenateNodes;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.PrerequisiteOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class ConcatenateNodes extends MutateNode<ConcatenateNodes.Builder, ConcatenateNodes>
        implements HighLevelRequest<TReqConcatenateNodes.Builder> {
    private final List<YPath> sourcePaths;
    private final YPath destinationPath;

    public ConcatenateNodes(BuilderBase<?> builder) {
        super(builder);
        this.sourcePaths = Objects.requireNonNull(builder.sourcePaths);
        this.destinationPath = Objects.requireNonNull(builder.destinationPath);
    }

    public ConcatenateNodes(String[] from, String to) {
        this(
                Arrays.stream(from).map(YPath::simple).collect(Collectors.toList()),
                YPath.simple(to)
        );
    }

    public ConcatenateNodes(List<YPath> source, YPath dest) {
        this(builder().setSourcePaths(source).setDestinationPath(dest));
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<YPath> getSourcePaths() {
        return sourcePaths;
    }

    public YPath getDestinationPath() {
        return destinationPath;
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqConcatenateNodes.Builder, ?> requestBuilder) {
        TReqConcatenateNodes.Builder builder = requestBuilder.body();
        for (YPath s : sourcePaths) {
            builder.addSrcPaths(s.toString());
        }

        builder.setDstPath(destinationPath.toString());

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
                .append(Arrays.toString(sourcePaths.toArray()))
                .append("; DstPath: ")
                .append(destinationPath)
                .append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder
                .apply(super::toTree)
                .key("source_paths").value(sourcePaths, (b2, t) -> t.toTree(b2))
                .key("destination_path").apply(destinationPath::toTree);
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setSourcePaths(sourcePaths)
                .setDestinationPath(destinationPath)
                .setTransactionalOptions(transactionalOptions != null
                        ? new TransactionalOptions(transactionalOptions)
                        : null)
                .setPrerequisiteOptions(prerequisiteOptions != null
                        ? new PrerequisiteOptions(prerequisiteOptions)
                        : null)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);

        builder.setMutatingOptions(new MutatingOptions(mutatingOptions));
        return builder;
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends MutateNode.Builder<TBuilder, ConcatenateNodes> {
        @Nullable
        private List<YPath> sourcePaths;
        @Nullable
        private YPath destinationPath;

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
            if (builder.sourcePaths != null) {
                this.sourcePaths = new ArrayList<>(builder.sourcePaths);
            }
            this.destinationPath = builder.destinationPath;
        }

        public TBuilder setSourcePaths(List<YPath> sourcePaths) {
            this.sourcePaths = sourcePaths;
            return self();
        }

        public TBuilder setDestinationPath(YPath destinationPath) {
            this.destinationPath = destinationPath;
            return self();
        }

        public List<YPath> getSourcePaths() {
            return Objects.requireNonNull(sourcePaths);
        }

        public YPath getDestinationPath() {
            return Objects.requireNonNull(destinationPath);
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            Objects.requireNonNull(sourcePaths);
            Objects.requireNonNull(destinationPath);
            sb
                    .append("SourcePaths: ")
                    .append(Arrays.toString(sourcePaths.toArray()))
                    .append("; DstPath: ")
                    .append(destinationPath.toString())
                    .append("; ");
            super.writeArgumentsLogString(sb);
        }

        @Override
        public YTreeBuilder toTree(YTreeBuilder builder) {
            Objects.requireNonNull(sourcePaths);
            Objects.requireNonNull(destinationPath);
            return builder
                    .apply(super::toTree)
                    .key("source_paths").value(sourcePaths, (b2, t) -> t.toTree(b2))
                    .key("destination_path").apply(destinationPath::toTree);
        }

        @Override
        public ConcatenateNodes build() {
            return new ConcatenateNodes(this);
        }
    }
}
