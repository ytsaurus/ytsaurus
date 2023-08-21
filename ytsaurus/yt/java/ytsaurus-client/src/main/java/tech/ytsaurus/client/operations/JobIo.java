package tech.ytsaurus.client.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;


/**
 * Immutable job I/O options.
 */
@NonNullApi
@NonNullFields
public class JobIo {
    private final boolean enableTableIndex;
    private final boolean enableRowIndex;
    private final @Nullable
    TableWriterOptions tableWriter;

    /**
     * Create job I/O with all options set to defaults.
     */
    public JobIo() {
        this(builder());
    }

    /**
     * Create job I/O options from table writer options with other options set to defaults.
     */
    public JobIo(TableWriterOptions tableWriter) {
        this(builder().setTableWriter(tableWriter));
    }

    protected <T extends BuilderBase<T>> JobIo(BuilderBase<T> builder) {
        enableTableIndex = builder.enableTableIndex;
        enableRowIndex = builder.enableRowIndex;
        tableWriter = builder.tableWriter;
    }

    /**
     * @see Builder#setTableWriter(TableWriterOptions)
     */
    public Optional<TableWriterOptions> getTableWriter() {
        return Optional.ofNullable(tableWriter);
    }

    /**
     * @see Builder#setEnableTableIndex(boolean)
     */
    public boolean isEnableTableIndex() {
        return enableTableIndex;
    }

    /**
     * @see Builder#setEnableRowIndex(boolean)
     */
    public boolean isEnableRowIndex() {
        return enableRowIndex;
    }

    /**
     * Construct a builder with options set from this request.
     */
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

    /**
     * Convert job I/O options to yson.
     */
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

    /**
     * Builder of {@link JobIo}.
     */
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
        private @Nullable
        TableWriterOptions tableWriter;

        /**
         * Create instance of {@link JobIo}.
         */
        public JobIo build() {
            return new JobIo(this);
        }

        /**
         * Set if actual table index will be available in OperationContext.
         */
        public T setEnableTableIndex(boolean enableTableIndex) {
            this.enableTableIndex = enableTableIndex;
            return self();
        }

        /**
         * Set if actual row index will be available in OperationContext.
         */
        public T setEnableRowIndex(boolean enableRowIndex) {
            this.enableRowIndex = enableRowIndex;
            return self();
        }

        /**
         * Set operation of table writer.
         */
        public T setTableWriter(@Nullable TableWriterOptions tableWriter) {
            this.tableWriter = tableWriter;
            return self();
        }

        protected abstract T self();
    }
}
