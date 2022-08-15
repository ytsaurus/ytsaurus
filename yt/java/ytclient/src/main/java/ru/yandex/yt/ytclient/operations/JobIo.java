package ru.yandex.yt.ytclient.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class JobIo {
    private final boolean enableTableIndex;
    private final boolean enableRowIndex;
    private final @Nullable
    TableWriterOptions tableWriter;

    public JobIo() {
        this(builder());
    }

    public JobIo(@Nullable TableWriterOptions tableWriter) {
        this(builder().setTableWriter(tableWriter));
    }

    protected <T extends BuilderBase<T>> JobIo(BuilderBase<T> builder) {
        enableTableIndex = builder.enableTableIndex;
        enableRowIndex = builder.enableRowIndex;
        tableWriter = builder.tableWriter;
    }

    public Optional<TableWriterOptions> getTableWriter() {
        return Optional.ofNullable(tableWriter);
    }

    public boolean isEnableTableIndex() {
        return enableTableIndex;
    }

    public boolean isEnableRowIndex() {
        return enableRowIndex;
    }

    public YTreeMapNode prepare() {
        return YTree.mapBuilder()
                .when(tableWriter != null, b -> b.key("table_writer").value(tableWriter.prepare()))
                .when(
                        enableTableIndex || enableRowIndex,
                        b -> b.key("control_attributes")
                                .beginMap()
                                .key("enable_table_index").value(enableTableIndex)
                                .key("enable_row_index").value(enableRowIndex)
                                .endMap()
                )
                .buildMap();
    }

    public static BuilderBase<?> builder() {
        return new Builder();
    }

    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> {
        private boolean enableTableIndex = false;
        private boolean enableRowIndex = false;
        private @Nullable TableWriterOptions tableWriter;

        public JobIo build() {
            return new JobIo(this);
        }

        public T setEnableTableIndex(boolean enableTableIndex) {
            this.enableTableIndex = enableTableIndex;
            return self();
        }

        public T setEnableRowIndex(boolean enableRowIndex) {
            this.enableRowIndex = enableRowIndex;
            return self();
        }

        public T setTableWriter(@Nullable TableWriterOptions tableWriter) {
            this.tableWriter = tableWriter;
            return self();
        }

        protected abstract T self();
    }
}
