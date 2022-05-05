package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

/**
 * Row modification request that uses YTreeObject annotated classes as table row representation
 *
 * @param <T> YTreeObject class
 *
 * @see ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject
 * @see ModifyRowsRequest
 */
public class MappedModifyRowsRequest<T> extends PreparableModifyRowsRequest<MappedModifyRowsRequest<T>> {
    private final List<T> rows = new ArrayList<>();
    private final WireRowSerializer<T> serializer;
    private boolean hasDeletes;
    private final ArrayList<Boolean> rowAggregates = new ArrayList<>();

    public MappedModifyRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedModifyRowsRequest(String path, WireRowSerializer<T> serializer) {
        super(path, serializer.getSchema());
        this.serializer = Objects.requireNonNull(serializer);
    }

    public void addInsert(T value) {
        this.addInsert(value, false);
    }

    public void addInserts(Iterable<? extends T> values) {
        this.addInserts(values, false);
    }

    public void addInsert(T value, boolean aggregate) {
        this.addImpl(value, ERowModificationType.RMT_WRITE, aggregate);
    }

    public void addInserts(Iterable<? extends T> values, boolean aggregate) {
        this.addImpl(values, ERowModificationType.RMT_WRITE, aggregate);
    }

    // @see #addInsert
    @Deprecated
    public void addUpdate(T value) {
        this.addInsert(value);
    }

    // @see #addInserts
    @Deprecated
    public void addUpdates(Iterable<? extends T> values) {
        this.addInserts(values);
    }

    public void addDelete(T value) {
        this.addImpl(value, ERowModificationType.RMT_DELETE, false);
        hasDeletes = true;
    }

    public void addDeletes(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_DELETE, false);
        hasDeletes = true;
    }

    //

    private void addImpl(T value, ERowModificationType type, boolean aggregate) {
        this.rows.add(value);
        this.rowModificationTypes.add(type);
        this.rowAggregates.add(aggregate);
    }

    @SuppressWarnings("unchecked")
    private void addImpl(Iterable<? extends T> values, ERowModificationType type, boolean aggregate) {
        if (values instanceof Collection) {
            this.addImpl((Collection<? extends T>) values, type, aggregate);
        } else {
            for (T value : values) {
                this.addImpl(value, type, aggregate);
            }
        }
    }

    private void addImpl(Collection<? extends T> values, ERowModificationType type, boolean aggregate) {
        this.rows.addAll(values);
        this.rowModificationTypes.ensureCapacity(this.rowModificationTypes.size() + values.size());
        this.rowAggregates.ensureCapacity(this.rowAggregates.size() + values.size());
        for (T ignored : values) {
            this.rowModificationTypes.add(type);
            this.rowAggregates.add(aggregate);
        }
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

    @Nonnull
    @Override
    protected MappedModifyRowsRequest<T> self() {
        return this;
    }
}
