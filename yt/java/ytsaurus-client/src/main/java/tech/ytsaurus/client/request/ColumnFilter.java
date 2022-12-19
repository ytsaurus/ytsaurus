package tech.ytsaurus.client.request;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TLegacyAttributeKeys;
import tech.ytsaurus.ysontree.YTreeBuilder;

public class ColumnFilter {
    @Nullable
    private Collection<String> columns;

    public ColumnFilter() {
    }

    public boolean isAllColumns() {
        return columns == null;
    }

    @Nullable
    public Collection<String> getColumns() {
        return columns;
    }

    public static ColumnFilter of(String... columns) {
        ColumnFilter result = new ColumnFilter();
        result.columns = Arrays.asList(columns);
        return result;
    }

    public ColumnFilter setColumns(@Nullable Collection<String> columns) {
        this.columns = columns;
        return this;
    }

    public TLegacyAttributeKeys.Builder writeTo(TLegacyAttributeKeys.Builder builder) {
        if (columns == null) {
            builder.setAll(true);
        } else {
            builder.addAllKeys(columns);
        }
        return builder;
    }

    public boolean isPresent() {
        return columns != null;
    }

    @Override
    public String toString() {
        if (columns == null) {
            return "all";
        }
        return columns.toString();
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.value(columns);
    }
}
