package tech.ytsaurus.core.tables;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Description of the column by which sorting occurs
 */
@NonNullFields
@NonNullApi
public class SortColumn {
    final String name;
    final ColumnSortOrder sortOrder;

    public SortColumn(String name) {
        this(name, ColumnSortOrder.ASCENDING);
    }

    public SortColumn(String name, ColumnSortOrder sortOrder) {
        this.name = name;
        this.sortOrder = sortOrder;
    }

    /**
     * @return name of the column
     */
    public String getName() {
        return name;
    }

    /**
     * @return sort order - descending and ascending
     */
    public ColumnSortOrder getSortOrder() {
        return sortOrder;
    }

    /**
     * Helper for converting column names
     */
    public static List<SortColumn> convert(Collection<String> names) {
        return names.stream()
                .map(name -> new SortColumn(name, ColumnSortOrder.ASCENDING))
                .collect(Collectors.toList());
    }

    /**
     * For converting to yson representation
     */
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.beginMap()
                .key("name").value(name)
                .key("sort_order").value(sortOrder.getName())
                .endMap();
    }
}
