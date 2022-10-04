package ru.yandex.yt.ytclient.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqReadFile;
import ru.yandex.yt.rpcproxy.TSuppressableAccessTrackingOptions;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.proxy.request.SuppressableAccessTrackingOptions;
import ru.yandex.yt.ytclient.proxy.request.TransactionalOptions;

public class ReadFile extends RequestBase<ReadFile.Builder, ReadFile> {
    private final String path;

    private Long offset = null;
    private Long length = null;
    private YTreeNode config = null;

    private TransactionalOptions transactionalOptions = null;
    private SuppressableAccessTrackingOptions suppressableAccessTrackingOptions = null;

    public ReadFile(BuilderBase<?, ?> builder) {
        super(builder);
        this.path = builder.path;
        this.offset = builder.offset;
        this.length = builder.length;
        if (builder.config != null) {
            this.config = YTree.deepCopy(builder.config);
        } else {
            this.config = null;
        }
        if (builder.transactionalOptions != null) {
            this.transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
        }
        if (builder.suppressableAccessTrackingOptions != null) {
            this.suppressableAccessTrackingOptions =
                    new SuppressableAccessTrackingOptions(builder.suppressableAccessTrackingOptions);
        } else {
            this.suppressableAccessTrackingOptions = null;
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

    public static class Builder extends BuilderBase<Builder, ReadFile> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public ReadFile build() {
            return new ReadFile(this);
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends RequestBase<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
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
    }
}
