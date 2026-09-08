package tech.ytsaurus.client.request;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TReqWriteShuffleData;

public class CreateShuffleWriter extends RequestBase<CreateShuffleWriter.Builder, CreateShuffleWriter> {
    private final ShuffleHandle handle;
    private final String partitionColumn;
    @Nullable
    private final Integer writerIndex;
    @Nullable
    private final Boolean overwriteExistingWriterData;

    private final long windowSize;
    private final long packetSize;

    public CreateShuffleWriter(BuilderBase<?> builder) {
        super(builder);
        this.handle = builder.handle;
        this.partitionColumn = builder.partitionColumn;
        this.writerIndex = builder.writerIndex;
        this.overwriteExistingWriterData = builder.overwriteExistingWriterData;
        this.windowSize = builder.windowSize;
        this.packetSize = builder.packetSize;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public static CreateShuffleWriter.Builder builder() {
        return new CreateShuffleWriter.Builder();
    }

    public void writeTo(TReqWriteShuffleData.Builder builder) {
        builder.setSignedShuffleHandle(handle.getPayload());
        builder.setPartitionColumn(partitionColumn);
        if (writerIndex != null) {
            builder.setWriterIndex(writerIndex);
        }
        if (overwriteExistingWriterData != null) {
            builder.setOverwriteExistingWriterData(overwriteExistingWriterData);
        }
    }

    @Override
    public CreateShuffleWriter.Builder toBuilder() {
        return builder()
                .setHandle(handle)
                .setPartitionColumn(partitionColumn)
                .setWriterIndex(writerIndex)
                .setOverwriteExistingWriterData(overwriteExistingWriterData)
                .setWindowSize(windowSize)
                .setPacketSize(packetSize);
    }

    public static class Builder extends CreateShuffleWriter.BuilderBase<CreateShuffleWriter.Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends CreateShuffleWriter.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, CreateShuffleWriter> {
        private ShuffleHandle handle;
        private String partitionColumn;
        @Nullable
        private Integer writerIndex;
        @Nullable
        private Boolean overwriteExistingWriterData;

        private long windowSize = 16000000L;
        private long packetSize = windowSize / 2;

        public TBuilder setHandle(ShuffleHandle handle) {
            this.handle = handle;
            return self();
        }

        public TBuilder setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
            return self();
        }

        public TBuilder setWriterIndex(@Nullable Integer writerIndex) {
            this.writerIndex = writerIndex;
            return self();
        }

        public TBuilder setOverwriteExistingWriterData(@Nullable Boolean overwriteExistingWriterData) {
            this.overwriteExistingWriterData = overwriteExistingWriterData;
            return self();
        }

        public TBuilder setWindowSize(long windowSize) {
            this.windowSize = windowSize;
            return self();
        }

        public TBuilder setPacketSize(long packetSize) {
            this.packetSize = packetSize;
            return self();
        }

        public CreateShuffleWriter build() {
            return new CreateShuffleWriter(this);
        }
    }
}
