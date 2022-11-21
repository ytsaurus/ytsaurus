package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rows.MappedRowSerializer;
import tech.ytsaurus.client.rows.WireProtocolWriter;
import tech.ytsaurus.client.rows.WireRowSerializer;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.SerializationResolver;

/**
 * Row modification request that uses YTreeObject annotated classes as table row representation
 *
 * @param <T> YTreeObject class
 *
 * @see ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject
 * @see ModifyRowsRequest
 */
public class MappedModifyRowsRequest<T>
        extends PreparableModifyRowsRequest<MappedModifyRowsRequest.Builder<T>, MappedModifyRowsRequest<T>> {
    private final List<T> rows;
    private final WireRowSerializer<T> serializer;
    private final boolean hasDeletes;
    private final ArrayList<Boolean> rowAggregates;

    public MappedModifyRowsRequest(BuilderBase<T, ?> builder) {
        super(builder);
        this.rows = new ArrayList<>(builder.rows);
        this.serializer = Objects.requireNonNull(builder.serializer);
        this.hasDeletes = builder.hasDeletes;
        this.rowAggregates = new ArrayList<>(builder.rowAggregates);
    }

    public MappedModifyRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(MappedModifyRowsRequest.<T>builder().setPath(path).setSerializer(serializer));
    }

    public MappedModifyRowsRequest(String path, WireRowSerializer<T> serializer) {
        this(MappedModifyRowsRequest.<T>builder().setPath(path).setSerializer(serializer));
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public void convertValues(SerializationResolver serializationResolver) {
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        final WireProtocolWriter writer = new WireProtocolWriter(attachments);
        if (hasDeletes) {
            writer.writeUnversionedRowset(rows, serializer,
                    i -> rowModificationTypes.get(i) == ERowModificationType.RMT_DELETE,
                    rowAggregates::get);
        } else {
            writer.writeUnversionedRowset(rows, serializer, (i) -> false, rowAggregates::get);
        }
        writer.finish();
    }

    @Override
    public Builder<T> toBuilder() {
        return MappedModifyRowsRequest.<T>builder()
                .setRows(rows)
                .setRowAggregates(rowAggregates)
                .setHasDeletes(hasDeletes)
                .setPath(path)
                .setSchema(schema)
                .setRequireSyncReplica(requireSyncReplica)
                .setRowModificationTypes(rowModificationTypes)
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

        @Override
        public MappedModifyRowsRequest<T> build() {
            return new MappedModifyRowsRequest<T>(this);
        }
    }

    public abstract static class BuilderBase<
            T, TBuilder extends BuilderBase<T, TBuilder>>
            extends PreparableModifyRowsRequest.Builder<TBuilder, MappedModifyRowsRequest<T>> {
        private final List<T> rows = new ArrayList<>();
        @Nullable
        private WireRowSerializer<T> serializer;
        private boolean hasDeletes;
        private final ArrayList<Boolean> rowAggregates = new ArrayList<>();

        public TBuilder setSerializer(YTreeObjectSerializer<T> serializer) {
            this.serializer = MappedRowSerializer.forClass(serializer);
            setSchema(this.serializer.getSchema());
            return self();
        }

        public TBuilder setSerializer(WireRowSerializer<T> serializer) {
            this.serializer = serializer;
            setSchema(this.serializer.getSchema());
            return self();
        }

        public TBuilder addInsert(T value) {
            return addInsert(value, false);
        }

        public TBuilder addInserts(Iterable<? extends T> values) {
            return addInserts(values, false);
        }

        public TBuilder addInsert(T value, boolean aggregate) {
            return addImpl(value, ERowModificationType.RMT_WRITE, aggregate);
        }

        public TBuilder addInserts(Iterable<? extends T> values, boolean aggregate) {
            return addImpl(values, ERowModificationType.RMT_WRITE, aggregate);
        }

        // @see #addInsert
        @Deprecated
        public TBuilder addUpdate(T value) {
            return addInsert(value);
        }

        // @see #addInserts
        @Deprecated
        public TBuilder addUpdates(Iterable<? extends T> values) {
            return addInserts(values);
        }

        public TBuilder addDelete(T value) {
            addImpl(value, ERowModificationType.RMT_DELETE, false);
            hasDeletes = true;
            return self();
        }

        public TBuilder addDeletes(Iterable<? extends T> values) {
            addImpl(values, ERowModificationType.RMT_DELETE, false);
            hasDeletes = true;
            return self();
        }

        //

        private TBuilder addImpl(T value, ERowModificationType type, boolean aggregate) {
            this.rows.add(value);
            addRowModificationType(type);
            this.rowAggregates.add(aggregate);
            return self();
        }

        @SuppressWarnings("unchecked")
        private TBuilder addImpl(Iterable<? extends T> values, ERowModificationType type, boolean aggregate) {
            if (values instanceof Collection) {
                return addImpl((Collection<? extends T>) values, type, aggregate);
            } else {
                for (T value : values) {
                    this.addImpl(value, type, aggregate);
                }
                return self();
            }
        }

        TBuilder setRows(List<T> rows) {
            this.rows.addAll(rows);
            return self();
        }

        TBuilder setRowAggregates(List<Boolean> rowAggregates) {
            this.rowAggregates.addAll(rowAggregates);
            return self();
        }

        TBuilder setHasDeletes(boolean hasDeletes) {
            this.hasDeletes = hasDeletes;
            return self();
        }

        private TBuilder addImpl(Collection<? extends T> values, ERowModificationType type, boolean aggregate) {
            this.rows.addAll(values);
            this.rowAggregates.ensureCapacity(this.rowAggregates.size() + values.size());
            for (T ignored : values) {
                addRowModificationType(type);
                this.rowAggregates.add(aggregate);
            }
            return self();
        }
    }
}
