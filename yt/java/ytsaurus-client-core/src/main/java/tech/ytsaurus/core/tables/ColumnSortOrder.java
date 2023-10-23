package tech.ytsaurus.core.tables;

import java.util.HashMap;
import java.util.Map;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * ESortOrder (yt/ytlib/table_client/schema.h)
 */
@NonNullApi
@NonNullFields
public enum ColumnSortOrder {
    ASCENDING("ascending", 0),
    DESCENDING("descending", 1);

    private static final Map<String, ColumnSortOrder> MAP_FROM_NAME = new HashMap<>();
    private static final Map<Integer, ColumnSortOrder> MAP_FROM_ID = new HashMap<>();

    private final String name;
    private final Integer id;

    ColumnSortOrder(String name, int id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public Integer getId() {
        return id;
    }

    public static ColumnSortOrder fromName(String name) {
        ColumnSortOrder sortOrder = MAP_FROM_NAME.get(name);
        if (sortOrder == null) {
            throw new IllegalArgumentException("Unsupported sort order " + name);
        }
        return sortOrder;
    }

    public static ColumnSortOrder fromId(int id) {
        ColumnSortOrder sortOrder = MAP_FROM_ID.get(id);
        if (sortOrder == null) {
            throw new IllegalArgumentException("Unsupported sort order " + id);
        }
        return sortOrder;
    }

    static {
        for (ColumnSortOrder sortOrder : values()) {
            MAP_FROM_NAME.put(sortOrder.getName(), sortOrder);
            MAP_FROM_ID.put(sortOrder.getId(), sortOrder);
        }
    }
}
