package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.TableAttachmentByteBufferPartitionReader;
import tech.ytsaurus.client.TableAttachmentReader;
import tech.ytsaurus.client.TableAttachmentSkiffPartitionReader;
import tech.ytsaurus.client.TablePartitionRowsetReader;
import tech.ytsaurus.client.rows.EntitySkiffSerializer;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowDeserializer;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TReqReadTablePartition;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;


import static tech.ytsaurus.client.rows.EntityUtil.isEntityAnnotationPresent;

public class CreateTablePartitionReader<T>
        extends RequestBase<CreateTablePartitionReader.Builder<T>, CreateTablePartitionReader<T>> {
    private final SerializationContext<T> serializationContext;
    @Nullable
    private final TablePartitionCookie cookie;
    private final boolean unordered;
    private final boolean omitInaccessibleColumns;
    private final boolean enableTableIndex;
    private final boolean enableRowIndex;
    private final boolean enableRangeIndex;
    @Nullable
    private final YTreeNode config;

    protected CreateTablePartitionReader(BuilderBase<T, ?> builder) {
        super(builder);
        this.cookie = builder.cookie;
        this.serializationContext = Objects.requireNonNull(builder.serializationContext);
        this.unordered = builder.unordered;
        this.omitInaccessibleColumns = builder.omitInaccessibleColumns;
        this.enableTableIndex = builder.enableTableIndex;
        this.enableRowIndex = builder.enableRowIndex;
        this.enableRangeIndex = builder.enableRangeIndex;
        this.config = builder.config;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static Builder<ByteBuffer> binaryArrowBuilder() {
        SerializationContext<ByteBuffer> context = new ReadSerializationContext<>(
                ERowsetFormat.RF_ARROW,
                new TableAttachmentByteBufferPartitionReader()
        );
        return new Builder<ByteBuffer>().setSerializationContext(context);
    }

    public static <T> Builder<T> builder(Class<T> rowClass) {
        SerializationContext<T> context;
        if (rowClass.equals(UnversionedRow.class)) {
            context = new ReadSerializationContext<>(
                    (TableAttachmentReader<T>) new TablePartitionRowsetReader<>(new UnversionedRowDeserializer())
            );
        } else if (isEntityAnnotationPresent(rowClass)) {
            context = new SerializationContext<>(
                    rowClass,
                    new TableAttachmentSkiffPartitionReader<>(new EntitySkiffSerializer<>(rowClass))
            );
        } else {
            throw new IllegalArgumentException("Unsupported class for CreateTablePartitionReader builder: "
                    + rowClass.getName());
        }

        return new Builder<T>().setSerializationContext(context);
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    public void writeTo(TReqReadTablePartition.Builder builder) {
        builder.setCookie(cookie.getPayload());
        builder.setUnordered(unordered);
        builder.setOmitInaccessibleColumns(omitInaccessibleColumns);
        builder.setEnableTableIndex(enableTableIndex);
        builder.setEnableRowIndex(enableRowIndex);
        builder.setEnableRangeIndex(enableRangeIndex);
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            builder.setConfig(ByteString.copyFrom(baos.toByteArray()));
        }

        builder.setDesiredRowsetFormat(serializationContext.getRowsetFormat());

        if (serializationContext.getFormat().isPresent()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(serializationContext.getFormat().get().toTree(), baos);
            builder.setFormat(ByteString.copyFrom(baos.toByteArray()));
        }
    }

    @Override
    public Builder<T> toBuilder() {
        return new Builder<T>()
                .setCookie(cookie)
                .setSerializationContext(serializationContext)
                .setUnordered(unordered)
                .setOmitInaccessibleColumns(omitInaccessibleColumns)
                .setEnableTableIndex(enableTableIndex)
                .setEnableRowIndex(enableRowIndex)
                .setEnableRangeIndex(enableRangeIndex)
                .setConfig(config);
    }

    public static class Builder<T> extends BuilderBase<T, Builder<T>> {
        @Override
        protected Builder<T> self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            T, TBuilder extends BuilderBase<T, TBuilder>>
            extends RequestBase.Builder<TBuilder, CreateTablePartitionReader<T>> {
        @Nullable
        private TablePartitionCookie cookie;
        @Nullable
        private SerializationContext<T> serializationContext;
        private boolean unordered = false;
        private boolean omitInaccessibleColumns = false;
        private boolean enableTableIndex = false;
        private boolean enableRowIndex = false;
        private boolean enableRangeIndex = false;
        @Nullable
        private YTreeNode config = null;

        public TBuilder setCookie(TablePartitionCookie cookie) {
            this.cookie = cookie;
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

        public TBuilder setEnableTableIndex(boolean enableTableIndex) {
            this.enableTableIndex = enableTableIndex;
            return self();
        }

        public TBuilder setEnableRowIndex(boolean enableRowIndex) {
            this.enableRowIndex = enableRowIndex;
            return self();
        }

        public TBuilder setEnableRangeIndex(boolean enableRangeIndex) {
            this.enableRangeIndex = enableRangeIndex;
            return self();
        }

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        @Override
        public CreateTablePartitionReader<T> build() {
            return new CreateTablePartitionReader<>(this);
        }
    }
}
