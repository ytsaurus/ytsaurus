package tech.ytsaurus.client.rows;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.ytclient.tables.TableSchema;

@NonNullApi
public interface WireRowDeserializer<T> {
    WireValueDeserializer<?> onNewRow(int columnCount);

    T onCompleteRow();

    @Nullable
    T onNullRow();

    default void updateSchema(TableSchema schema) { }
}
