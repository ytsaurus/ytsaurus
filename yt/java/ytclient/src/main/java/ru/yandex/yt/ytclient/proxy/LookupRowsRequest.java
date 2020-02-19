package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

public class LookupRowsRequest extends RequestBase<LookupRowsRequest> {
    private final String path;
    private final TableSchema schema;
    private final List<String> lookupColumns = new ArrayList<>();
    private final List<UnversionedRow> filters = new ArrayList<>();
    private YtTimestamp timestamp;
    private Boolean keepMissingRows = null;

    public LookupRowsRequest(String path, TableSchema schema) {
        if (!schema.isLookupSchema()) {
            throw new IllegalArgumentException("LookupRowsRequest requires a lookup schema");
        }
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
    }

    public String getPath() {
        return path;
    }

    public Optional<Boolean> getKeepMissingRows() {
        return Optional.ofNullable(keepMissingRows);
    }

    public LookupRowsRequest setKeepMissingRows(boolean keepMissingRows) {
        this.keepMissingRows = keepMissingRows;
        return this;
    }

    public LookupRowsRequest setTimestamp(YtTimestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<String> getLookupColumns() {
        return Collections.unmodifiableList(lookupColumns);
    }

    public LookupRowsRequest addLookupColumn(String name) {
        lookupColumns.add(name);
        return this;
    }

    public LookupRowsRequest addLookupColumns(List<String> names) {
        lookupColumns.addAll(names);
        return this;
    }

    public LookupRowsRequest addLookupColumns(String... names) {
        return addLookupColumns(Arrays.asList(names));
    }

    private UnversionedRow convertFilterToRow(List<?> filter) {
        if (filter.size() != schema.getColumns().size()) {
            throw new IllegalArgumentException("Number of filter columns must match the number key columns");
        }
        List<UnversionedValue> row = new ArrayList<>(schema.getColumns().size());
        ApiServiceUtil.convertKeyColumns(row, schema, filter);
        return new UnversionedRow(row);
    }

    public LookupRowsRequest addFilter(List<?> filter) {
        filters.add(convertFilterToRow(Objects.requireNonNull(filter)));
        return this;
    }

    public LookupRowsRequest addFilter(Object... filterValues) {
        return addFilter(Arrays.asList(filterValues));
    }

    public LookupRowsRequest addFilters(Iterable<? extends List<?>> filters) {
        for (List<?> filter : filters) {
            addFilter(filter);
        }
        return this;
    }

    public void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(filters, new UnversionedRowSerializer(schema));
        writer.finish();
    }
}
