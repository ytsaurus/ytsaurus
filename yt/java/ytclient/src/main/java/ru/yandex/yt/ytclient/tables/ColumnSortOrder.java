package ru.yandex.yt.ytclient.tables;

import java.util.HashMap;
import java.util.Map;

/**
 * ESortOrder (yt/ytlib/table_client/schema.h)
 */
public enum ColumnSortOrder {
    ASCENDING("ascending");

    private final String name;

    ColumnSortOrder(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ColumnSortOrder fromName(String name) {
        ColumnSortOrder sortOrder = mapFromName.get(name);
        if (sortOrder == null) {
            throw new IllegalArgumentException("Unsupport sort order " + name);
        }
        return sortOrder;
    }

    private static final Map<String, ColumnSortOrder> mapFromName = new HashMap<>();

    static {
        for (ColumnSortOrder sortOrder : values()) {
            mapFromName.put(sortOrder.getName(), sortOrder);
        }
    }
}
