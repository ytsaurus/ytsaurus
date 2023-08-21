package tech.ytsaurus.client.rows;

import javax.annotation.Nullable;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public interface WireRowDeserializer<T> {
    WireValueDeserializer<?> onNewRow(int columnCount);

    T onCompleteRow();

    @Nullable
    T onNullRow();

    default void updateSchema(TableSchema schema) {
    }
}
