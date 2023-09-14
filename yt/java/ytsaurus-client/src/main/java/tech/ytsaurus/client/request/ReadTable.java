package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
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
    @Nullable
    private final TableSchema tableSchema;
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
        this.tableSchema = builder.tableSchema;
        this.unordered = builder.unordered;
        this.omitInaccessibleColumns = builder.omitInaccessibleColumns;
        this.config = builder.config;
        this.transactionalOptions = builder.transactionalOptions;
    }

    public ReadTable(YPath path, SerializationContext<T> serializationContext) {
        this(new Builder<T>()
                .setPath(path)
                .setSerializationContext(serializationContext));
    }

    public ReadTable(YPath path, Class<T> objectClass) {
        this(new Builder<T>()
                .setPath(path)
                .setSerializationContext(new SerializationContext<>(objectClass)));
    }

    /**
     * Use {@link #builder(Class)} instead if you don't need specific SerializationContext.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static <T> Builder<T> builder(Class<T> rowClass) {
        return new Builder<T>().setSerializationContext(new SerializationContext<>(rowClass));
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    public YPath getYPath() {
        return Objects.requireNonNull(path);
    }

    public Optional<TableSchema> getTableSchema() {
        return Optional.ofNullable(tableSchema);
    }

    public Optional<GUID> getTransactionId() {
        if (this.transactionalOptions == null) {
            return Optional.empty();
        }
        return this.transactionalOptions.getTransactionId();
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
        return new Builder<T>()
                .setPath(path)
                .setPath(stringPath)
                .setSerializationContext(serializationContext)
                .setTableSchema(tableSchema)
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
        private Builder() {
        }

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
        @Nullable
        private TableSchema tableSchema;
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

        /**
         * @deprecated prefer to use {@link #setPath(YPath)}
         */
        @Deprecated
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

        public TBuilder setTableSchema(@Nullable TableSchema tableSchema) {
            this.tableSchema = tableSchema;
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
