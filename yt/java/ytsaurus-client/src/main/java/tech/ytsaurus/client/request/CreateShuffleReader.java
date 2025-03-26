package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.rpcproxy.TReqReadShuffleData;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class CreateShuffleReader extends RequestBase<CreateShuffleReader.Builder, CreateShuffleReader> {
    private final ShuffleHandle handle;
    private final int partitionIndex;
    @Nullable
    private final YTreeNode config;

    private static final YTreeNode EMPTY_CONFIG = YTree.builder().beginMap().endMap().build();

    public CreateShuffleReader(BuilderBase<?> builder) {
        super(builder);
        this.handle = builder.handle;
        this.partitionIndex = builder.partitionIndex;
        this.config = builder.config;
    }

    public static CreateShuffleReader.Builder builder() {
        return new CreateShuffleReader.Builder();
    }

    public void writeTo(TReqReadShuffleData.Builder builder) {
        builder.setShuffleHandle(handle.getPayload());
        builder.setPartitionIndex(partitionIndex);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        YTreeBinarySerializer.serialize(this.config == null ? EMPTY_CONFIG : this.config, baos);
        byte[] data = baos.toByteArray();
        builder.setReaderConfig(ByteString.copyFrom(data));
    }

    @Override
    public CreateShuffleReader.Builder toBuilder() {
        return builder()
                .setHandle(handle)
                .setPartitionIndex(partitionIndex)
                .setConfig(config);
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
        private YTreeNode config = null;

        public TBuilder setHandle(ShuffleHandle handle) {
            this.handle = handle;
            return self();
        }

        public TBuilder setPartitionIndex(int partitionIndex) {
            this.partitionIndex = partitionIndex;
            return self();
        }

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        public CreateShuffleReader build() {
            return new CreateShuffleReader(this);
        }
    }
}
