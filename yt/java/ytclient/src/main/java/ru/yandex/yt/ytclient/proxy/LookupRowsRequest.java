package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.ytclient.object.UnversionedRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

@NonNullApi
public class LookupRowsRequest extends AbstractLookupRowsRequest<LookupRowsRequest> {
    private final List<UnversionedRow> filters = new ArrayList<>();

    public LookupRowsRequest(String path, TableSchema schema) {
        super(path, schema);
    }

    private UnversionedRow convertFilterToRow(List<?> filter) {
        TableSchema schema = getSchema();
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

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(filters, new UnversionedRowSerializer(getSchema()));
        writer.finish();
    }

    @Nonnull
    @Override
    protected LookupRowsRequest self() {
        return this;
    }
}
