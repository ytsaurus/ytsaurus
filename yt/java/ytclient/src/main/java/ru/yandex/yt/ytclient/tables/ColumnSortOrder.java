package ru.yandex.yt.ytclient.tables;

import java.util.HashMap;
import java.util.Map;

/**
 * ESortOrder (yt/ytlib/table_client/schema.h)
 */
public enum ColumnSortOrder {
    ASCENDING("ascending");

    private static final Map<String, ColumnSortOrder> MAP_FROM_NAME = new HashMap<>();

    private final String name;

    ColumnSortOrder(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ColumnSortOrder fromName(String name) {
        ColumnSortOrder sortOrder = MAP_FROM_NAME.get(name);
        if (sortOrder == null) {
            throw new IllegalArgumentException("Unsupport sort order " + name);
        }
        return sortOrder;
    }

    static {
        for (ColumnSortOrder sortOrder : values()) {
            MAP_FROM_NAME.put(sortOrder.getName(), sortOrder);
        }
    }
}
