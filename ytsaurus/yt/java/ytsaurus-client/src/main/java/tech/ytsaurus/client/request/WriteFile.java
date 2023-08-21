package tech.ytsaurus.client.request;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TPrerequisiteOptions;
import tech.ytsaurus.rpcproxy.TReqWriteFile;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeNode;

public class WriteFile extends RequestBase<WriteFile.Builder, WriteFile> {
    private final String path;
    @Nullable
    private final YTreeNode config;
    @Nullable
    private final Boolean computeMd5;
    @Nullable
    private final TransactionalOptions transactionalOptions;
    @Nullable
    private final PrerequisiteOptions prerequisiteOptions;
    private final long windowSize;
    private final long packetSize;

    public WriteFile(BuilderBase<?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.config = builder.config;
        this.computeMd5 = builder.computeMd5;
        if (builder.transactionalOptions != null) {
            this.transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
        } else {
            this.transactionalOptions = null;
        }
        if (builder.prerequisiteOptions != null) {
            this.prerequisiteOptions = new PrerequisiteOptions(builder.prerequisiteOptions);
        } else {
            this.prerequisiteOptions = null;
        }
        this.windowSize = builder.windowSize;
        this.packetSize = builder.packetSize;
    }

    public WriteFile(String path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    public YPath getPath() {
        return YPath.simple(path);
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public TReqWriteFile.Builder writeTo(TReqWriteFile.Builder builder) {
        builder.setPath(path);

        if (computeMd5 != null) {
            builder.setComputeMd5(computeMd5);
        }
        if (config != null) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            YTreeBinarySerializer.serialize(config, baos);
            byte[] data = baos.toByteArray();
            builder.setConfig(ByteString.copyFrom(data));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (prerequisiteOptions != null) {
            builder.setPrerequisiteOptions(prerequisiteOptions.writeTo(TPrerequisiteOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
        return builder;
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setConfig(config)
                .setComputeMd5(computeMd5)
                .setTransactionalOptions(transactionalOptions)
                .setPrerequisiteOptions(prerequisiteOptions)
                .setWindowSize(windowSize)
                .setPacketSize(packetSize);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, WriteFile> {
        @Nullable
        private String path;

        @Nullable
        private YTreeNode config = null;

        @Nullable
        private Boolean computeMd5 = null;

        @Nullable
        private TransactionalOptions transactionalOptions = null;
        @Nullable
        private PrerequisiteOptions prerequisiteOptions = null;

        private long windowSize = 16000000L;
        private long packetSize = windowSize / 2;

        public TBuilder setPath(String path) {
            this.path = path;
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

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions to) {
            this.transactionalOptions = to;
            return self();
        }

        public TBuilder setPrerequisiteOptions(@Nullable PrerequisiteOptions prerequisiteOptions) {
            this.prerequisiteOptions = prerequisiteOptions;
            return self();
        }

        public TBuilder setComputeMd5(@Nullable Boolean flag) {
            this.computeMd5 = flag;
            return self();
        }

        @Override
        public WriteFile build() {
            return new WriteFile(this);
        }
    }
}
