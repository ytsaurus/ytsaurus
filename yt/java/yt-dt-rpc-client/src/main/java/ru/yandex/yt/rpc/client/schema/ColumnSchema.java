package ru.yandex.yt.rpc.client.schema;

import ru.yandex.yt.rpc.client.ValueType;

/**
 * @author valri
 */
public class ColumnSchema {
    public Short id;
    public String name;
    public ValueType valueType;
    public Boolean toLookup;

    public ColumnSchema(short id, String name, ValueType valueType) {
        this(id, name, valueType, true);
    }

    public ColumnSchema(short id, String name, ValueType valueType, Boolean toLookup) {
        this.id = id;
        this.name = name;
        this.valueType = valueType;
        this.toLookup = toLookup;
    }
}
