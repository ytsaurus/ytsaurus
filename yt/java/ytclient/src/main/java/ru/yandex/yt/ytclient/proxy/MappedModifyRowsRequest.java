package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class MappedModifyRowsRequest<T> extends AbstractModifyRowsRequest {

    private final List<T> rows = new ArrayList<>();
    private final WireRowSerializer<T> serializer;

    public MappedModifyRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedModifyRowsRequest(String path, WireRowSerializer<T> serializer) {
        super(path, serializer.getSchema());
        this.serializer = Objects.requireNonNull(serializer);
    }

    @Override
    public MappedModifyRowsRequest<T> setRequireSyncReplica(boolean requireSyncReplica) {
        super.setRequireSyncReplica(requireSyncReplica);
        return this;
    }

    public void addInsert(T value) {
        this.addImpl(value, ERowModificationType.RMT_WRITE);
    }

    public void addInserts(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addUpdate(T value) {
        this.addImpl(value, ERowModificationType.RMT_WRITE);
    }

    public void addUpdates(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addDelete(T value) {
        this.addImpl(value, ERowModificationType.RMT_DELETE);
    }

    public void addDeletes(Iterable<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_DELETE);
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
        writer.writeUnversionedRowset(rows, serializer);
        writer.finish();
    }
}
