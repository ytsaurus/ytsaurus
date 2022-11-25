package tech.ytsaurus.core.rows;

import java.lang.reflect.Field;

import javax.annotation.Nullable;


import tech.ytsaurus.core.tables.ColumnSortOrder;
/**
 * @author sankear
 */
@SuppressWarnings("VisibilityModifier")
public class YTreeObjectField<T> {

    public final Field field;
    public final boolean isAttribute;
    public final String key;
    public final YTreeSerializer<T> serializer;
    public final boolean isFlatten;
    @Nullable public final ColumnSortOrder sortOrder;
    public final boolean isSaveAlways;
    @Nullable public final String aggregate;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YTreeObjectField(
            Field field,
            boolean isAttribute,
            String key,
            YTreeSerializer<T> serializer,
            boolean isFlatten,
            @Nullable ColumnSortOrder sortOrder,
            boolean isSaveAlways,
            @Nullable String aggregate
    ) {
        this.field = field;
        this.isAttribute = isAttribute;
        this.key = key;
        this.serializer = serializer;
        this.isFlatten = isFlatten;
        this.sortOrder = sortOrder;
        this.isSaveAlways = isSaveAlways;
        this.aggregate = aggregate;
    }
}
