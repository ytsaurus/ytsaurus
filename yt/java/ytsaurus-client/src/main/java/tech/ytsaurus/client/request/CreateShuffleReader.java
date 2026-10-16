package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TReqReadShuffleData;
import tech.ytsaurus.rpcproxy.TReqReadShuffleData.TIndexRange;

public class CreateShuffleReader extends RequestBase<CreateShuffleReader.Builder, CreateShuffleReader> {
    private final ShuffleHandle handle;
    private final int partitionIndex;
    @Nullable
    private final Range range;

    public CreateShuffleReader(BuilderBase<?> builder) {
        super(builder);
        this.handle = builder.handle;
        this.partitionIndex = builder.partitionIndex;
        this.range = builder.range;
    }

    public static CreateShuffleReader.Builder builder() {
        return new CreateShuffleReader.Builder();
    }

    public void writeTo(TReqReadShuffleData.Builder builder) {
        builder.setSignedShuffleHandle(handle.getPayload());
        builder.setPartitionIndex(partitionIndex);
        if (range != null) {
            TIndexRange indexRange = TIndexRange.newBuilder().setBegin(range.begin).setEnd(range.end).build();
            builder.setWriterIndexRange(indexRange);
        }
    }

    @Override
    public CreateShuffleReader.Builder toBuilder() {
        return builder()
                .setHandle(handle)
                .setPartitionIndex(partitionIndex)
                .setRange(range);
    }

    public static class Builder extends CreateShuffleReader.BuilderBase<CreateShuffleReader.Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends CreateShuffleReader.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, CreateShuffleReader> {
        private ShuffleHandle handle;
        private int partitionIndex;
        @Nullable
        private Range range = null;

        public TBuilder setHandle(ShuffleHandle handle) {
            this.handle = handle;
            return self();
        }

        public TBuilder setPartitionIndex(int partitionIndex) {
            this.partitionIndex = partitionIndex;
            return self();
        }

        public TBuilder setRange(@Nullable Range range) {
            this.range = range;
            return self();
        }

        public CreateShuffleReader build() {
            return new CreateShuffleReader(this);
        }
    }

    public static class Range {
        private final int begin;
        private final int end;

        public Range(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }
    }
}
