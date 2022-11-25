package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.client.TableAttachmentWireProtocolReader;
import tech.ytsaurus.client.rows.WireRowDeserializer;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeSerializer;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.yt.rpcproxy.ERowsetFormat;
import ru.yandex.yt.rpcproxy.TReqReadTable;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

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
        this(ReadTable.<T>builder().setPath(path).setSerializationContext(serializationContext));
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
        if (serializationContext.format != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(serializationContext.format.toTree(), baos);
            byte[] data = baos.toByteArray();
            builder.setFormat(ByteString.copyFrom(data));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        builder.setDesiredRowsetFormat(serializationContext.desiredRowsetFormat);
        if (serializationContext.desiredRowsetFormat == ERowsetFormat.RF_FORMAT) {
            if (serializationContext.format == null) {
                throw new IllegalStateException("`format` is required for desiredRowsetFormat == RF_FORMAT");
            }
        }
        return builder;
    }

    public static class SerializationContext<T> {
        private static final String YSON = "yson";

        @Nullable
        private final WireRowDeserializer<T> deserializer;
        @Nullable
        private final YTreeSerializer<T> serializer;
        @Nullable
        private final Class<T> objectClazz;
        private ERowsetFormat desiredRowsetFormat = ERowsetFormat.RF_YT_WIRE;
        @Nullable
        private Format format = null;
        @Nullable
        private TableAttachmentReader<T> attachmentReader = null;

        public SerializationContext(TableAttachmentReader<T> attachmentReader) {
            this.attachmentReader = attachmentReader;
            this.deserializer = null;
            this.serializer = null;
            this.objectClazz = null;
        }

        public SerializationContext(WireRowDeserializer<T> deserializer) {
            this(new TableAttachmentWireProtocolReader<>(deserializer));
        }

        public SerializationContext(YTreeSerializer<T> serializer) {
            this.serializer = serializer;
            this.deserializer = null;
            this.objectClazz = null;
        }

        public SerializationContext(Format format, TableAttachmentReader<T> attachmentReader) {
            this.deserializer = null;
            this.serializer = null;
            this.objectClazz = null;
            this.format = format;
            if (!format.getType().equals(YSON)) {
                throw new IllegalArgumentException("Unknown format: " + format.getType());
            }
            this.desiredRowsetFormat = ERowsetFormat.RF_FORMAT;
            this.attachmentReader = attachmentReader;
        }

        public SerializationContext(Class<T> objectClazz) {
            this.deserializer = null;
            this.serializer = null;
            this.objectClazz = objectClazz;
        }

        private SerializationContext(ERowsetFormat desiredRowsetFormat) {
            this.deserializer = null;
            this.serializer = null;
            this.objectClazz = null;
            this.desiredRowsetFormat = desiredRowsetFormat;
        }

        public static <T> SerializationContext<T> binaryArrow() {
            return new SerializationContext<>(ERowsetFormat.RF_ARROW);
        }

        public Optional<WireRowDeserializer<T>> getDeserializer() {
            return Optional.ofNullable(this.deserializer);
        }

        public Optional<YTreeSerializer<T>> getSerializer() {
            return Optional.ofNullable(this.serializer);
        }

        public Optional<Class<T>> getObjectClazz() {
            return Optional.ofNullable(this.objectClazz);
        }

        public Optional<TableAttachmentReader<T>> getAttachmentReader() {
            return Optional.ofNullable(attachmentReader);
        }
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
