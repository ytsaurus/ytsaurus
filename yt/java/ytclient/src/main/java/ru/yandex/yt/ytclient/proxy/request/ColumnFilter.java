package ru.yandex.yt.ytclient.proxy.request;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TAttributeKeys;

@NonNullApi
@NonNullFields
public class ColumnFilter {
    @Nullable private Collection<String> columns;

    public ColumnFilter() {
    }

    /**
     * @deprecated If all = true, use {@link #ColumnFilter().setColumns(null)} instead.
     * If all = false, use {@link #ColumnFilter().setColumns(columns)} instead.
     */
    @Deprecated
    public ColumnFilter(boolean all, List<String> columns) {
        if (all) {
            this.columns = null;
        } else {
            this.columns = columns;
        }
    }

    public static ColumnFilter of(String... columns) {
        ColumnFilter result = new ColumnFilter();
        result.columns = Arrays.asList(columns);
        return result;
    }

    /**
     * @deprecated Use {@link #setColumns(null)} instead.
     */
    @Deprecated
    public ColumnFilter setAll(boolean all) {
        if (all) {
            this.columns = null;
        } else {
            if (this.columns == null) {
                this.columns = Arrays.asList();
            }
        }
        return this;
    }

    public ColumnFilter setColumns(@Nullable Collection<String> columns) {
        this.columns = columns;
        return this;
    }

    public TAttributeKeys.Builder writeTo(TAttributeKeys.Builder builder) {
        if (columns == null) {
            builder.setAll(true);
        } else {
            builder.addAllColumns(columns);
        }
        return builder;
    }

    boolean isPresent() {
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
