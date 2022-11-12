package ru.yandex.yt.ytclient.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.EOperationType;
import ru.yandex.yt.rpcproxy.TMutatingOptions;
import ru.yandex.yt.rpcproxy.TReqStartOperation;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.MutatingOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class StartOperation extends RequestBase<StartOperation.Builder, StartOperation>
        implements HighLevelRequest<TReqStartOperation.Builder> {
    private final EOperationType type;
    private final YTreeNode spec;

    @Nullable
    private final TransactionalOptions transactionalOptions;
    private final MutatingOptions mutatingOptions;

    public StartOperation(BuilderBase<?> builder) {
        super(builder);
        this.type = Objects.requireNonNull(builder.type);
        this.spec = Objects.requireNonNull(builder.spec);
        this.transactionalOptions = builder.transactionalOptions;
        this.mutatingOptions = builder.mutatingOptions;
    }

    public StartOperation(EOperationType type, YTreeNode spec) {
        this(builder().setType(type).setSpec(spec));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartOperation.Builder, ?> requestBuilder) {
        TReqStartOperation.Builder builder = requestBuilder.body();
        ByteString.Output output = ByteString.newOutput();
        YTreeBinarySerializer.serialize(spec, output);

        builder
                .setType(type)
                .setSpec(output.toByteString());

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }

        builder.setMutatingOptions(mutatingOptions.writeTo(TMutatingOptions.newBuilder()));
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        sb.append("OperationType: ").append(type).append("; ");
        super.writeArgumentsLogString(sb);
    }

    @Override
    public void writeHeaderTo(TRequestHeader.Builder header) {
        super.writeHeaderTo(header);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setType(type)
                .setSpec(spec)
                .setTransactionalOptions(transactionalOptions)
                .setMutatingOptions(mutatingOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, StartOperation> {
        @Nullable
        private EOperationType type;
        @Nullable
        private YTreeNode spec;
        @Nullable
        private TransactionalOptions transactionalOptions;
        private MutatingOptions mutatingOptions = new MutatingOptions().setMutationId(GUID.create());

        protected BuilderBase() {
        }

        BuilderBase(BuilderBase<?> builder) {
            super(builder);
            type = builder.type;
            spec = YTree.deepCopy(spec);
            if (transactionalOptions != null) {
                transactionalOptions = new TransactionalOptions(transactionalOptions);
            }
            mutatingOptions = new MutatingOptions(mutatingOptions);
        }

        public TBuilder setType(EOperationType type) {
            this.type = type;
            return self();
        }

        public TBuilder setSpec(YTreeNode spec) {
            this.spec = spec;
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        public TBuilder setMutatingOptions(MutatingOptions mutatingOptions) {
            this.mutatingOptions = mutatingOptions;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            sb.append("OperationType: ").append(type).append("; ");
            super.writeArgumentsLogString(sb);
        }

        @Override
        public void writeHeaderTo(TRequestHeader.Builder header) {
            super.writeHeaderTo(header);
        }

        @Override
        public StartOperation build() {
            return new StartOperation(this);
        }
    }
}
