package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.rpcproxy.TReqReadFile;
import tech.ytsaurus.rpcproxy.TSuppressableAccessTrackingOptions;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class ReadFile extends RequestBase<ReadFile.Builder, ReadFile> {
    private final String path;

    @Nullable
    private final Long offset;
    @Nullable
    private final Long length;
    @Nullable
    private final YTreeNode config;

    @Nullable
    private final TransactionalOptions transactionalOptions;
    @Nullable
    private final SuppressableAccessTrackingOptions suppressableAccessTrackingOptions;

    public ReadFile(BuilderBase<?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.offset = builder.offset;
        this.length = builder.length;
        if (builder.config != null) {
            this.config = YTree.deepCopy(builder.config);
        } else {
            this.config = null;
        }
        if (builder.transactionalOptions == null) {
            this.transactionalOptions = null;
        } else {
            this.transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
        }
        if (builder.suppressableAccessTrackingOptions == null) {
            this.suppressableAccessTrackingOptions = null;
        } else {
            this.suppressableAccessTrackingOptions =
                    new SuppressableAccessTrackingOptions(builder.suppressableAccessTrackingOptions);
        }
    }

    public ReadFile(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    public TReqReadFile.Builder writeTo(TReqReadFile.Builder builder) {
        builder.setPath(path);
        if (offset != null) {
            builder.setOffset(offset);
        }
        if (length != null) {
            builder.setLength(length);
        }
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            builder.setConfig(ByteString.copyFrom(data));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (suppressableAccessTrackingOptions != null) {
            builder.setSuppressableAccessTrackingOptions(
                    suppressableAccessTrackingOptions.writeTo(TSuppressableAccessTrackingOptions.newBuilder())
            );
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setConfig(config)
                .setLength(length)
                .setOffset(offset)
                .setTransactionalOptions(transactionalOptions)
                .setSuppressableAccessTrackingOptions(suppressableAccessTrackingOptions)
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

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, ReadFile> {
        @Nullable
        private String path;

        @Nullable
        private Long offset = null;
        @Nullable
        private Long length = null;
        @Nullable
        private YTreeNode config = null;

        @Nullable
        private TransactionalOptions transactionalOptions = null;
        @Nullable
        private SuppressableAccessTrackingOptions suppressableAccessTrackingOptions = null;

        public TBuilder setPath(String path) {
            this.path = path;
            return self();
        }

        public TBuilder setOffset(@Nullable Long offset) {
            this.offset = offset;
            return self();
        }

        public TBuilder setLength(@Nullable Long length) {
            this.length = length;
            return self();
        }

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        public TBuilder setSuppressableAccessTrackingOptions(
                @Nullable SuppressableAccessTrackingOptions suppressableAccessTrackingOptions) {
            this.suppressableAccessTrackingOptions = suppressableAccessTrackingOptions;
            return self();
        }

        @Override
        public ReadFile build() {
            return new ReadFile(this);
        }
    }
}
