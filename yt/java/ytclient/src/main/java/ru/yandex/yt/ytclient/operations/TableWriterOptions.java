package ru.yandex.yt.ytclient.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.DataSize;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

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

    public static Builder builder() {
        return new Builder();
    }

    public Optional<DataSize> getMaxRowWeight() {
        return Optional.ofNullable(maxRowWeight);
    }

    public Optional<DataSize> getBlockSize() {
        return Optional.ofNullable(blockSize);
    }

    public Optional<DataSize> getDesiredChunkSize() {
        return Optional.ofNullable(desiredChunkSize);
    }

    public YTreeMapNode prepare() {
        return YTree.mapBuilder()
                .when(maxRowWeight != null, b -> b.key("max_row_weight").value(maxRowWeight.toBytes()))
                .when(blockSize != null, b -> b.key("block_size").value(blockSize.toBytes()))
                .when(desiredChunkSize != null,
                        b -> b.key("desired_chunk_size").value(desiredChunkSize.toBytes()))
                .buildMap();
    }

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

        public Builder setMaxRowWeight(@Nullable DataSize maxRowWeight) {
            this.maxRowWeight = maxRowWeight;
            return this;
        }

        public Builder setBlockSize(@Nullable DataSize blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder setDesiredChunkSize(@Nullable DataSize desiredChunkSize) {
            this.desiredChunkSize = desiredChunkSize;
            return this;
        }

        public TableWriterOptions build() {
            return new TableWriterOptions(this);
        }
    }
}
