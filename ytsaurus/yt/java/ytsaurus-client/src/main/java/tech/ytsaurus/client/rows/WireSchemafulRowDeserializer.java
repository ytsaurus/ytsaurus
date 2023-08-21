package tech.ytsaurus.client.rows;

import java.util.List;

public interface WireSchemafulRowDeserializer<T> extends WireRowDeserializer<T> {

    List<WireColumnSchema> getColumnSchema();
}
