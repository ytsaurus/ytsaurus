package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Iterator;
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

    public void addInsert(Iterator<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addInserts(List<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addUpdate(T value) {
        this.addImpl(value, ERowModificationType.RMT_WRITE);
    }

    public void addUpdates(Iterator<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addUpdates(List<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_WRITE);
    }

    public void addDelete(T value) {
        this.addImpl(value, ERowModificationType.RMT_DELETE);
    }

    public void addDeletes(Iterator<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_DELETE);
    }

    public void addDeletes(List<? extends T> values) {
        this.addImpl(values, ERowModificationType.RMT_DELETE);
    }

    //

    private void addImpl(T value, ERowModificationType type) {
        this.rows.add(value);
        this.rowModificationTypes.add(type);
    }

    private void addImpl(Iterator<? extends T> values, ERowModificationType type) {
        values.forEachRemaining(value -> addImpl(value, type));
    }

    private void addImpl(List<? extends T> values, ERowModificationType type) {
        this.rows.addAll(values);
        values.stream().map(v -> type).forEach(this.rowModificationTypes::add);
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        final WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows, serializer);
        writer.finish();
    }
}
