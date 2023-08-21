package tech.ytsaurus.client.rows;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;

public interface WireRowSerializer<T> {

    TableSchema getSchema();

    void serializeRow(T row, WireProtocolWriteable writeable, boolean keyFieldsOnly, boolean aggregate,
                      int[] idMapping);

    // TODO: use TableSchema type here
    default void updateSchema(TRowsetDescriptor schemaDelta) {

    }
}
