package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeSerializer;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.WireRowSerializer;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class WriteTable<T> extends ru.yandex.yt.ytclient.request.WriteTable.BuilderBase<
        T, WriteTable<T>, ru.yandex.yt.ytclient.request.WriteTable<T>> {

    public WriteTable(YPath path, WireRowSerializer<T> serializer, @Nullable TableSchema tableSchema) {
        setSerializationContext(new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(serializer));
        setPath(path).setTableSchema(tableSchema);
    }

    public WriteTable(YPath path, WireRowSerializer<T> serializer) {
        setPath(path);
        setSerializationContext(new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(serializer));
    }

    public WriteTable(YPath path, YTreeSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(
                        MappedRowSerializer.forClass(serializer)));
    }

    public WriteTable(YPath path, YTreeSerializer<T> serializer, Format format) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(serializer, format));
    }

    public WriteTable(YPath path, Class<T> objectClazz, @Nullable TableSchema tableSchema) {
       setPath(path).setTableSchema(tableSchema).setSerializationContext(
               new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(objectClazz));
    }

    public WriteTable(YPath path, Class<T> objectClazz) {
        this(path, objectClazz, null);
    }


    /**
     * @deprecated Use {@link #WriteTable(YPath path, WireRowSerializer<T> serializer)} instead.
     */
    @Deprecated
    public WriteTable(String path, WireRowSerializer<T> serializer) {
        setPath(path).setSerializationContext(
                new ru.yandex.yt.ytclient.request.WriteTable.SerializationContext<T>(serializer));
    }

    /**
     * @deprecated Use {@link #WriteTable(YPath path, YTreeSerializer<T> serializer)} instead.
     */
    @Deprecated
    public WriteTable(String path, YTreeSerializer<T> serializer) {
        this(path, MappedRowSerializer.forClass(serializer));
    }

    @Nonnull
    @Override
    protected WriteTable<T> self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.WriteTable<T> build() {
        return new ru.yandex.yt.ytclient.request.WriteTable<>(this);
    }
}
