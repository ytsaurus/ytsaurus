package ru.yandex.yt.ytclient.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

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

    public JobIo(TableWriterOptions tableWriter) {
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

    public BuilderBase<?> toBuilder() {
        BuilderBase<?> result = builder()
                .setEnableRowIndex(isEnableRowIndex())
                .setEnableTableIndex(isEnableTableIndex());
        if (getTableWriter().isPresent()) {
            result.setTableWriter(TableWriterOptions.builder()
                    .setMaxRowWeight(getTableWriter().get().getMaxRowWeight().orElse(null))
                    .setBlockSize(getTableWriter().get().getBlockSize().orElse(null))
                    .setDesiredChunkSize(getTableWriter().get().getDesiredChunkSize().orElse(null))
                    .build());
        }
        return result;
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

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same JobIo class.
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
