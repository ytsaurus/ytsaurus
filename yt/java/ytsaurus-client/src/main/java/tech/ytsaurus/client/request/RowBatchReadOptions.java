package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.rpcproxy.TRowBatchReadOptions;

public class RowBatchReadOptions {
    private final Long maxRowCount;
    private final DataSize maxDataWeight;
    @Nullable
    private final DataSize dataWeightPerRowHint;

    private RowBatchReadOptions(Builder builder) {
        this.maxRowCount = Objects.requireNonNull(builder.maxRowCount);
        this.maxDataWeight = Objects.requireNonNull(builder.maxDataWeight);
        this.dataWeightPerRowHint = builder.dataWeightPerRowHint;
    }

    public TRowBatchReadOptions toProto() {
        TRowBatchReadOptions.Builder builder = TRowBatchReadOptions.newBuilder();
        builder.setMaxRowCount(maxRowCount);
        builder.setMaxDataWeight(maxDataWeight.toBytes());
        if (dataWeightPerRowHint != null) {
            builder.setDataWeightPerRowHint(dataWeightPerRowHint.toBytes());
        }
        return builder.build();
    }

    /**
     * Construct empty builder for row batch read options.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Long maxRowCount = 1000L;
        private DataSize maxDataWeight = DataSize.fromMegaBytes(16);
        @Nullable
        private DataSize dataWeightPerRowHint;

        private Builder() {
        }

        public Builder setMaxRowCount(long maxRowCount) {
            this.maxRowCount = maxRowCount;
            return this;
        }

        public Builder setMaxDataWeight(DataSize maxDataWeight) {
            this.maxDataWeight = maxDataWeight;
            return this;
        }

        public Builder setDataWeightPerRowHint(DataSize dataWeightPerRowHint) {
            this.dataWeightPerRowHint = dataWeightPerRowHint;
            return this;
        }

        /**
         * Construct {@link RowBatchReadOptions} instance.
         */
        public RowBatchReadOptions build() {
            return new RowBatchReadOptions(this);
        }
    }
}
