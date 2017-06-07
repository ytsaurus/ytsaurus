package ru.yandex.yt.rpc.client.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.yandex.yt.rpc.client.ValueType;

/**
 * @author valri
 */
public class TableSchema {
    public String path;
    public Map<Short, String> idToName;
    public Map<String, ValueType> nameToType;
    public Map<String, Short> initialSequence;
    public List<String> columnsToLookup;

    public TableSchema(String path, List<ColumnSchema> schema) {
        this.columnsToLookup = new ArrayList<>(schema.size());
        this.idToName = new HashMap<>(schema.size());
        this.nameToType = new HashMap<>(schema.size());
        this.initialSequence = new HashMap<>(schema.size());
        this.path = path;

        Collections.sort(schema, (o1, o2) -> o1.id - o2.id);
        for (int i = 0; i < schema.size(); ++i) {
            this.initialSequence.put(schema.get(i).name, (short) i);
            idToName.put(schema.get(i).id, schema.get(i).name);
            nameToType.put(schema.get(i).name, schema.get(i).valueType);
            if (schema.get(i).toLookup) {
                columnsToLookup.add(schema.get(i).name);
            }
        }
    }
}
