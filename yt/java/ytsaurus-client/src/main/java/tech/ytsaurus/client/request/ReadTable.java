package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TReqReadTable;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class ReadTable<T> extends RequestBase<ReadTable.Builder<T>, ReadTable<T>> {
    @Nullable
    private final YPath path;
    @Nullable
    private final String stringPath;
    private final SerializationContext<T> serializationContext;

    private final boolean unordered;
    private final boolean omitInaccessibleColumns;
    @Nullable
    private final YTreeNode config;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    public ReadTable(BuilderBase<T, ?> builder) {
        super(builder);
        if (builder.path == null && builder.stringPath == null) {
            throw new IllegalArgumentException("Path wasn't set");
        }
        this.path = builder.path;
        this.stringPath = builder.stringPath;
        this.serializationContext = Objects.requireNonNull(builder.serializationContext);
        this.unordered = builder.unordered;
        this.omitInaccessibleColumns = builder.omitInaccessibleColumns;
        this.config = builder.config;
        this.transactionalOptions = builder.transactionalOptions;
    }

    public ReadTable(YPath path, SerializationContext<T> serializationContext) {
        this(ReadTable.<T>builder()
                .setPath(path)
                .setSerializationContext(serializationContext));
    }

    public ReadTable(YPath path, Class<T> objectClass) {
        this(ReadTable.<T>builder()
                .setPath(path)
                .setSerializationContext(new SerializationContext<>(objectClass)));
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    private String getPath() {
        return path != null ? path.toString() : Objects.requireNonNull(stringPath);
    }

    public TReqReadTable.Builder writeTo(TReqReadTable.Builder builder) {
        builder.setUnordered(unordered);
        builder.setOmitInaccessibleColumns(omitInaccessibleColumns);
        builder.setPath(getPath());
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            builder.setConfig(ByteString.copyFrom(data));
        }
        if (serializationContext.getFormat().isPresent()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(serializationContext.getFormat().get().toTree(), baos);
            byte[] data = baos.toByteArray();
            builder.setFormat(ByteString.copyFrom(data));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        builder.setDesiredRowsetFormat(serializationContext.getRowsetFormat());
        if (serializationContext.getRowsetFormat() == ERowsetFormat.RF_FORMAT) {
            if (serializationContext.getFormat().isEmpty()) {
                throw new IllegalStateException("`format` is required for desiredRowsetFormat == RF_FORMAT");
            }
        }
        return builder;
    }

    @Override
    public Builder<T> toBuilder() {
        return ReadTable.<T>builder()
                .setPath(path)
                .setPath(stringPath)
                .setSerializationContext(serializationContext)
                .setUnordered(unordered)
                .setOmitInaccessibleColumns(omitInaccessibleColumns)
                .setConfig(config)
                .setTransactionalOptions(transactionalOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder<T> extends BuilderBase<T, Builder<T>> {
        @Override
        protected Builder<T> self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            T, TBuilder extends BuilderBase<T, TBuilder>>
            extends RequestBase.Builder<TBuilder, ReadTable<T>> {
        @Nullable
        private YPath path;
        @Nullable
        private String stringPath;
        @Nullable
        private SerializationContext<T> serializationContext;

        private boolean unordered = false;
        private boolean omitInaccessibleColumns = false;
        @Nullable
        private YTreeNode config = null;

        @Nullable
        private TransactionalOptions transactionalOptions = null;

        public TBuilder setPath(@Nullable YPath path) {
            this.path = path;
            return self();
        }

        public TBuilder setPath(@Nullable String stringPath) {
            this.stringPath = stringPath;
            return self();
        }

        public TBuilder setSerializationContext(SerializationContext<T> serializationContext) {
            if (serializationContext instanceof WriteSerializationContext) {
                throw new IllegalArgumentException("WriteSerializationContext do not allowed here");
            }
            this.serializationContext = serializationContext;
            return self();
        }

        public TBuilder setUnordered(boolean unordered) {
            this.unordered = unordered;
            return self();
        }

        public TBuilder setOmitInaccessibleColumns(boolean omitInaccessibleColumns) {
            this.omitInaccessibleColumns = omitInaccessibleColumns;
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

        @Override
        public ReadTable<T> build() {
            return new ReadTable<>(this);
        }
    }
}
