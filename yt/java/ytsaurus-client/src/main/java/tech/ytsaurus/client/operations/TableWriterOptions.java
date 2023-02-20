package tech.ytsaurus.client.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;


/**
 * Immutable table writer options.
 */
@NonNullApi
@NonNullFields
public class TableWriterOptions {
    @Nullable
    private final DataSize maxRowWeight;
    @Nullable
    private final DataSize blockSize;
    @Nullable
    private final DataSize desiredChunkSize;

    TableWriterOptions(Builder builder) {
        this.maxRowWeight = builder.maxRowWeight;
        this.blockSize = builder.blockSize;
        this.desiredChunkSize = builder.desiredChunkSize;
    }

    /**
     * Create empty builder of {@link TableWriterOptions}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @see Builder#setMaxRowWeight(DataSize)
     */
    public Optional<DataSize> getMaxRowWeight() {
        return Optional.ofNullable(maxRowWeight);
    }

    /**
     * @see Builder#setBlockSize(DataSize)
     */
    public Optional<DataSize> getBlockSize() {
        return Optional.ofNullable(blockSize);
    }

    /**
     * @see Builder#setDesiredChunkSize(DataSize)
     */
    public Optional<DataSize> getDesiredChunkSize() {
        return Optional.ofNullable(desiredChunkSize);
    }

    /**
     * Convert to yson.
     */
    public YTreeMapNode prepare() {
        return YTree.mapBuilder()
                .when(maxRowWeight != null, b -> b.key("max_row_weight").value(maxRowWeight.toBytes()))
                .when(blockSize != null, b -> b.key("block_size").value(blockSize.toBytes()))
                .when(desiredChunkSize != null,
                        b -> b.key("desired_chunk_size").value(desiredChunkSize.toBytes()))
                .buildMap();
    }

    /**
     * Builder of {@link TableWriterOptions}.
     */
    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        private DataSize maxRowWeight;
        @Nullable
        private DataSize blockSize;
        @Nullable
        private DataSize desiredChunkSize;

        Builder() {
        }

        /**
         * Set max weight of one row, it can't greater than 128 MB.
         * By default, 16 MB.
         */
        public Builder setMaxRowWeight(@Nullable DataSize maxRowWeight) {
            this.maxRowWeight = maxRowWeight;
            return this;
        }

        /**
         * Set block size, it can't be less than 1 KB.
         * By default, 16 MB.
         */
        public Builder setBlockSize(@Nullable DataSize blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Set desired chunk size.
         */
        public Builder setDesiredChunkSize(@Nullable DataSize desiredChunkSize) {
            this.desiredChunkSize = desiredChunkSize;
            return this;
        }

        /**
         * Create instance of {@link TableWriterOptions}.
         */
        public TableWriterOptions build() {
            return new TableWriterOptions(this);
        }
    }
}
