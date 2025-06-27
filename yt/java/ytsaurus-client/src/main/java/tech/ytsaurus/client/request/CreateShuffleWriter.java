package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.rpcproxy.TReqWriteShuffleData;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class CreateShuffleWriter extends RequestBase<CreateShuffleWriter.Builder, CreateShuffleWriter> {
    private final ShuffleHandle handle;
    private final String partitionColumn;
    @Nullable
    private final YTreeNode config;
    @Nullable
    private final Integer writerIndex;
    @Nullable
    private final Boolean overwriteExistingWriterData;

    private static final YTreeNode EMPTY_CONFIG = YTree.builder().beginMap().endMap().build();

    private final long windowSize;
    private final long packetSize;

    public CreateShuffleWriter(BuilderBase<?> builder) {
        super(builder);
        this.handle = builder.handle;
        this.partitionColumn = builder.partitionColumn;
        this.config = builder.config;
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(this.config == null ? EMPTY_CONFIG : this.config, baos);
        byte[] data = baos.toByteArray();
        builder.setWriterConfig(ByteString.copyFrom(data));
    }

    @Override
    public CreateShuffleWriter.Builder toBuilder() {
        return builder()
                .setHandle(handle)
                .setPartitionColumn(partitionColumn)
                .setConfig(config)
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
        private YTreeNode config = null;
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

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
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
