package ru.yandex.yt.ytclient.proxy;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class ModifyRowsRequest {
    private final String path;
    private final TableSchema schema;
    private Boolean requireSyncReplica = null;
    private final List<UnversionedRow> rows = new ArrayList<>();
    private final List<ERowModificationType> rowModificationTypes = new ArrayList<>();

    public ModifyRowsRequest(String path, TableSchema schema) {
        if (!schema.isWriteSchema()) {
            throw new IllegalArgumentException("ModifyRowsRequest requires a write schema");
        }
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
    }

    public String getPath() {
        return path;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<ERowModificationType> getRowModificationTypes() {
        return Collections.unmodifiableList(rowModificationTypes);
    }

    public ModifyRowsRequest setRequireSyncReplica(boolean requireSyncReplica) {
        this.requireSyncReplica = requireSyncReplica;
        return this;
    }

    public Optional<Boolean> getRequireSyncReplica() {
        return Optional.ofNullable(requireSyncReplica);
    }

    private UnversionedRow convertValuesToRow(List<?> values, boolean skipMissingValues, boolean aggregate) {
        if (values.size() < schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException(
                    "Number of values must be more than or equal to the number of key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(values.size());
        ApiServiceUtil.convertKeyColumns(row, schema, values);
        ApiServiceUtil.convertValueColumns(row, schema, values, skipMissingValues, aggregate);
        return new UnversionedRow(row);
    }

    public ModifyRowsRequest addInsert(List<?> values) {
        if (values.size() != schema.getColumns().size()) {
            throw new IllegalArgumentException("Number of insert columns must match number of schema columns");
        }
        rows.add(convertValuesToRow(values, false, false));
        rowModificationTypes.add(ERowModificationType.RMT_WRITE);
        return this;
    }

    public ModifyRowsRequest addInserts(Iterable<? extends List<?>> rows) {
        for (List<?> row : rows) {
            addInsert(row);
        }
        return this;
    }

    public ModifyRowsRequest addUpdate(List<?> values, boolean aggregate) {
        if (values.size() <= schema.getKeyColumnsCount() || values.size() > schema.getColumns().size()) {
            throw new IllegalArgumentException("Number of update columns must be more than the number of key columns");
        }
        rows.add(convertValuesToRow(values, true, aggregate));
        rowModificationTypes.add(ERowModificationType.RMT_WRITE);
        return this;
    }

    public ModifyRowsRequest addUpdates(Iterable<? extends List<?>> rows, boolean aggregate) {
        for (List<?> row : rows) {
            addUpdate(row, aggregate);
        }
        return this;
    }

    public ModifyRowsRequest addUpdate(List<?> values) {
        return addUpdate(values, false);
    }

    public ModifyRowsRequest addUpdates(Iterable<? extends List<?>> rows) {
        return addUpdates(rows, false);
    }

    public ModifyRowsRequest addDelete(List<?> values) {
        if (values.size() != schema.getKeyColumnsCount()) {
            throw new IllegalArgumentException("Number of delete columns must match number of key columns");
        }
        rows.add(convertValuesToRow(values, false, false));
        rowModificationTypes.add(ERowModificationType.RMT_DELETE);
        return this;
    }

    public ModifyRowsRequest addDeletes(Iterable<? extends List<?>> keys) {
        for (List<?> key : keys) {
            addDelete(key);
        }
        return this;
    }

    private List<Object> mapToValues(Map<String, ?> values, int size) {
        return new AbstractList<Object>() {
            @Override
            public Object get(int index) {
                return values.get(schema.getColumns().get(index).getName());
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    public ModifyRowsRequest addInsert(Map<String, ?> map) {
        return addInsert(mapToValues(map, schema.getColumnsCount()));
    }

    public ModifyRowsRequest addUpdate(Map<String, ?> map, boolean aggregate) {
        return addUpdate(mapToValues(map, schema.getColumnsCount()), aggregate);
    }

    public ModifyRowsRequest addUpdate(Map<String, ?> map) {
        return addUpdate(map, false);
    }

    public ModifyRowsRequest addDelete(Map<String, ?> map) {
        return addDelete(mapToValues(map, schema.getKeyColumnsCount()));
    }

    public void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows);
        writer.finish();
    }
}
