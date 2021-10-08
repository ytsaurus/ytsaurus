package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.WireProtocolWriter;

@NonNullApi
public class MappedLookupRowsRequest<T> extends AbstractLookupRowsRequest<MappedLookupRowsRequest<T>> {
    private final List<T> rows = new ArrayList<>();
    private final WireRowSerializer<T> serializer;
    private final TableSchema mappedSchema;

    public MappedLookupRowsRequest(String path, YTreeObjectSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    public MappedLookupRowsRequest(String path, WireRowSerializer<T> serializer) {
        super(path, serializer.getSchema().toLookup());
        this.mappedSchema = serializer.getSchema();
        this.serializer = Objects.requireNonNull(serializer);
    }


    public MappedLookupRowsRequest<T> addAllLookupColumns() {
        int count = mappedSchema.getColumnsCount();
        for (int i = 0; i < count; i++) {
            addLookupColumn(mappedSchema.getColumnName(i));
        }
        return this;
    }

    public MappedLookupRowsRequest<T> addKeyLookupColumns() {
        int count = mappedSchema.getKeyColumnsCount();
        for (int i = 0; i < count; i++) {
            addLookupColumn(mappedSchema.getColumnName(i));
        }
        return this;
    }

    public MappedLookupRowsRequest<T> addFilter(T filter) {
        rows.add(filter);
        return this;
    }

    public MappedLookupRowsRequest<T> addFilters(Iterable<? extends T> filters) {
        for (T filter : filters) {
            addFilter(filter);
        }
        return this;
    }

    @Override
    public void serializeRowsetTo(List<byte[]> attachments) {
        // Записываем только ключи
        WireProtocolWriter writer = new WireProtocolWriter(attachments);
        writer.writeUnversionedRowset(rows, serializer, i -> true);
        writer.finish();
    }

    @Nonnull
    @Override
    protected MappedLookupRowsRequest<T> self() {
        return this;
    }
}
