package ru.yandex.yt.ytclient.proxy.request;

import java.util.List;

import ru.yandex.yt.rpcproxy.TColumnFilter;

public class ColumnFilter {
    private Boolean all;
    private List<String> columns;

    public ColumnFilter() {}

    public ColumnFilter(boolean all, List<String> columns) {
        this.all = all;
        this.columns = columns;
    }

    public ColumnFilter setAll(boolean all) {
        this.all = all;
        return this;
    }

    public ColumnFilter setColumns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    public TColumnFilter.Builder writeTo(TColumnFilter.Builder builder) {
        if (all != null) {
            builder.setAll(all);
        }
        builder.addAllColumns(columns);
        return builder;
    }
}
