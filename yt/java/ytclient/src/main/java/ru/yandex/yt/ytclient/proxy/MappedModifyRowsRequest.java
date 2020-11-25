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

public class MappedModifyRowsRequest<T> extends AbstractModifyRowsRequest<MappedModifyRowsRequest<T>> {

    private final List<T> rows = new ArrayList<>();
    private final WireRowSerializer<T> serializer;
    private boolean hasDeletes;

    public MappedModifyRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedModifyRowsRequest(String path, WireRowSerializer<T> serializer) {
        super(path, serializer.getSchema());
        this.serializer = Objects.requireNonNull(serializer);
    }

    public void addInsert(T value) {
        this.addImpl(value, ERowModificationType.RMT_WRITE);
    }

    public void addInserts(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
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
        this.addImpl(value, ERowModificationType.RMT_DELETE);
        hasDeletes = true;
    }

    public void addDeletes(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_DELETE);
        hasDeletes = true;
    }

    //

    private void addImpl(T value, ERowModificationType type) {
        this.rows.add(value);
        this.rowModificationTypes.add(type);
    }

    @SuppressWarnings("unchecked")
    private void addImpl(Iterable<? extends T> values, ERowModificationType type) {
        if (values instanceof Collection) {
            this.addImpl((Collection<? extends T>) values, type);
        } else {
            for (T value : values) {
                this.addImpl(value, type);
            }
        }
    }

    private void addImpl(Collection<? extends T> values, ERowModificationType type) {
        this.rows.addAll(values);
        this.rowModificationTypes.ensureCapacity(this.rowModificationTypes.size() + values.size());
        for (T ignored : values) {
            this.rowModificationTypes.add(type);
        }
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        final WireProtocolWriter writer = new WireProtocolWriter(attachments);
        if (hasDeletes) {
            writer.writeUnversionedRowset(rows, serializer,
                    i -> rowModificationTypes.get(i) == ERowModificationType.RMT_DELETE);
        } else {
            writer.writeUnversionedRowset(rows, serializer);
        }
        writer.finish();
    }

    @Nonnull
    @Override
    protected MappedModifyRowsRequest<T> self() {
        return this;
    }
}
