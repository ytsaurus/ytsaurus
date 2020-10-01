package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.tables.TableSchema;

public abstract class AbstractLookupRowsRequest<R extends AbstractLookupRowsRequest<R>> extends RequestBase<R> {
    private final String path;
    private final TableSchema schema;
    private final List<String> lookupColumns = new ArrayList<>();
    private YtTimestamp timestamp;
    private Boolean keepMissingRows = null;

    public AbstractLookupRowsRequest(String path, TableSchema schema) {
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
        if (!schema.isLookupSchema()) {
            throw new IllegalArgumentException("LookupRowsRequest requires a lookup schema");
        }
    }

    public String getPath() {
        return path;
    }

    public Optional<Boolean> getKeepMissingRows() {
        return Optional.ofNullable(keepMissingRows);
    }

    @SuppressWarnings("unchecked")
    public R setKeepMissingRows(boolean keepMissingRows) {
        this.keepMissingRows = keepMissingRows;
        return (R) this;
    }

    @SuppressWarnings("unchecked")
    public R setTimestamp(YtTimestamp timestamp) {
        this.timestamp = timestamp;
        return (R) this;
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

    @SuppressWarnings("unchecked")
    public R addLookupColumn(String name) {
        lookupColumns.add(name);
        return (R) this;
    }

    @SuppressWarnings("unchecked")
    public R addLookupColumns(List<String> names) {
        lookupColumns.addAll(names);
        return (R) this;
    }

    public R addLookupColumns(String... names) {
        return addLookupColumns(Arrays.asList(names));
    }

    public abstract void serializeRowsetTo(List<byte[]> attachments);
}
