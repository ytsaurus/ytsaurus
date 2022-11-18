package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.yt.rpcproxy.ERowsetFormat;
import ru.yandex.yt.rpcproxy.TReqWriteTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WriteTable<T> extends RequestBase<WriteTable.Builder<T>, WriteTable<T>> {
    @Nullable
    private final YPath path;
    @Nullable
    private final String stringPath;
    private final SerializationContext<T> serializationContext;
    @Nullable
    private final TableSchema tableSchema;

    @Nullable
    private final YTreeNode config;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    private final long windowSize;
    private final long packetSize;

    private final boolean needRetries;
    private final int maxWritesInFlight;
    private final int chunkSize;

    public WriteTable(YPath path, SerializationContext<T> serializationContext) {
        this(WriteTable.<T>builder().setPath(path).setSerializationContext(serializationContext));
    }

    public WriteTable(YPath path, SerializationContext<T> serializationContext, TableSchema tableSchema) {
        this(WriteTable.<T>builder()
                .setPath(path)
                .setSerializationContext(serializationContext)
                .setTableSchema(tableSchema));
    }

    public WriteTable(BuilderBase<T, ?> builder) {
        super(builder);
        this.path = builder.path;
        this.stringPath = builder.stringPath;
        this.serializationContext = Objects.requireNonNull(builder.serializationContext);
        this.tableSchema = builder.tableSchema;
        this.config = builder.config;
        this.transactionalOptions = builder.transactionalOptions;
        this.windowSize = builder.windowSize;
        this.packetSize = builder.packetSize;
        this.needRetries = builder.needRetries;
        this.maxWritesInFlight = builder.maxWritesInFlight;
        this.chunkSize = builder.chunkSize;
    }

    public static <T> Builder<T> builder() {
        return new Builder<T>();
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    /**
     * @see BuilderBase.setNeedRetries(boolean)
     */
    public boolean getNeedRetries() {
        return needRetries;
    }

    /**
     * @see BuilderBase.setMaxWritesInFlight(int)
     */
    public int getMaxWritesInFlight() {
        return maxWritesInFlight;
    }

    /**
     * @see BuilderBase.setChunkSize(int)
     */
    public int getChunkSize() {
        return chunkSize;
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

    public String getPath() {
        return path != null ? path.toString() : Objects.requireNonNull(stringPath);
    }

    public YPath getYPath() {
        return Objects.requireNonNull(path);
    }

    public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
        builder.setPath(getPath());
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            builder.setConfig(ByteString.copyFrom(data));
        } else {
            // TODO: remove this HACK
            builder.setConfig(ByteString.copyFrom("{}", UTF_8));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        Optional<Format> format = serializationContext.getFormat();
        if (format.isPresent()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(format.get().toTree(), baos);
            byte[] data = baos.toByteArray();
            builder.setFormat(ByteString.copyFrom(data));
        }
        return builder;
    }

    public static class SerializationContext<T> {
        @Nullable
        private final WireRowSerializer<T> serializer;
        @Nullable
        private final YTreeSerializer<T> ysonSerializer;
        @Nullable
        private final Class<T> objectClazz;
        private ERowsetFormat rowsetFormat = ERowsetFormat.RF_YT_WIRE;
        @Nullable
        private Format format = null;

        public SerializationContext(WireRowSerializer<T> serializer) {
            this.serializer = serializer;
            this.objectClazz = null;
            this.ysonSerializer = null;
        }

        public SerializationContext(YTreeSerializer<T> serializer) {
            this.ysonSerializer = serializer;
            this.serializer = null;
            this.objectClazz = null;
        }

        public SerializationContext(YTreeSerializer<T> serializer, Format format) {
            if (!(serializer instanceof YTreeRowSerializer)) {
                throw new IllegalArgumentException("YTreeRowSerializer was expected");
            }
            this.ysonSerializer = serializer;
            this.serializer = null;
            this.objectClazz = null;
            this.rowsetFormat = ERowsetFormat.RF_FORMAT;
            this.format = format;
        }

        public SerializationContext(Class<T> objectClazz) {
            this.ysonSerializer = null;
            this.serializer = null;
            this.objectClazz = objectClazz;
        }

        public Optional<WireRowSerializer<T>> getSerializer() {
            return Optional.ofNullable(this.serializer);
        }

        public Optional<YTreeSerializer<T>> getYsonSerializer() {
            return Optional.ofNullable(this.ysonSerializer);
        }

        public Optional<Class<T>> getObjectClazz() {
            return Optional.ofNullable(this.objectClazz);
        }

        public ERowsetFormat getRowsetFormat() {
            return rowsetFormat;
        }

        public Optional<Format> getFormat() {
            return Optional.ofNullable(format);
        }
    }

    @Override
    public Builder<T> toBuilder() {
        return WriteTable.<T>builder()
                .setPath(path)
                .setPath(stringPath)
                .setSerializationContext(serializationContext)
                .setTableSchema(tableSchema)
                .setNeedRetries(needRetries)
                .setMaxWritesInFlight(maxWritesInFlight)
                .setChunkSize(chunkSize)
                .setWindowSize(windowSize)
                .setPacketSize(packetSize)
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
            T,
            TBuilder extends BuilderBase<T, TBuilder>>
            extends RequestBase.Builder<TBuilder, WriteTable<T>> {
        @Nullable
        private YPath path;
        @Nullable
        private String stringPath;

        @Nullable
        private SerializationContext<T> serializationContext;
        @Nullable
        private TableSchema tableSchema;

        @Nullable
        private YTreeNode config = null;
        @Nullable
        private TransactionalOptions transactionalOptions = null;

        private long windowSize = 16000000L;
        private long packetSize = windowSize / 2;

        private boolean needRetries = false;
        private int maxWritesInFlight = 1;
        private int chunkSize = 524288000;

        public TBuilder setPath(@Nullable YPath path) {
            this.path = path;
            return self();
        }

        public TBuilder setPath(@Nullable String path) {
            this.stringPath = path;
            return self();
        }

        public TBuilder setSerializationContext(SerializationContext<T> serializationContext) {
            this.serializationContext = serializationContext;
            return self();
        }

        public TBuilder setTableSchema(@Nullable TableSchema tableSchema) {
            this.tableSchema = tableSchema;
            return self();
        }

        /**
         * If you need a writer with retries, set needRetries=true.
         * RetryPolicy should be set in RpcOptions
         * @param needRetries
         * @return self
         */
        public TBuilder setNeedRetries(boolean needRetries) {
            this.needRetries = needRetries;
            return self();
        }

        /**
         * If a rows ordering doesn't matter, you can set maxWritesInFlight more than 1.
         * This will make writing faster.
         * @param maxWritesInFlight
         * @return self
         */
        public TBuilder setMaxWritesInFlight(int maxWritesInFlight) {
            this.maxWritesInFlight = maxWritesInFlight;
            return self();
        }

        /**
         * Allows to regular a chunk size in the output table.
         * @param chunkSize
         * @return self
         */
        public TBuilder setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return self();
        }

        public TBuilder setWindowSize(long windowSize) {
            this.windowSize = windowSize;
            return self();
        }

        public TBuilder setPacketSize(long packetSize) {
            this.packetSize = packetSize;
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

        public String getPath() {
            return path != null ? path.toString() : Objects.requireNonNull(stringPath);
        }

        public TReqWriteTable.Builder writeTo(TReqWriteTable.Builder builder) {
            builder.setPath(getPath());
            if (config != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                YTreeBinarySerializer.serialize(config, baos);
                byte[] data = baos.toByteArray();
                builder.setConfig(ByteString.copyFrom(data));
            } else {
                // TODO: remove this HACK
                builder.setConfig(ByteString.copyFrom("{}", UTF_8));
            }
            if (transactionalOptions != null) {
                builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
            }
            if (additionalData != null) {
                builder.mergeFrom(additionalData);
            }
            Optional<Format> format = Objects.requireNonNull(serializationContext).getFormat();
            if (format.isPresent()) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                YTreeBinarySerializer.serialize(format.get().toTree(), baos);
                byte[] data = baos.toByteArray();
                builder.setFormat(ByteString.copyFrom(data));
            }
            return builder;
        }

        @Override
        public WriteTable<T> build() {
            return new WriteTable<T>(this);
        }
    }
}
