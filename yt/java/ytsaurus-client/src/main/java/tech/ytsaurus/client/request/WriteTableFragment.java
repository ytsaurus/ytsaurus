package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.rpcproxy.TReqWriteTableFragment;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * A request to write table fragment for corresponding distributed write cookie.
 */
public class WriteTableFragment<T> extends RequestBase<WriteTableFragment.Builder<T>, WriteTableFragment<T>> {

    @Nullable
    private final YTreeNode config;

    @Nullable
    private final YTreeNode format;

    private final DistributedWriteCookie cookie;
    private final SerializationContext<T> serializationContext;

    private final long windowSize;
    private final long packetSize;

    public WriteTableFragment(BuilderBase<T, ?> builder) {
        super(builder);
        this.cookie = Objects.requireNonNull(builder.cookie);
        this.serializationContext = Objects.requireNonNull(builder.serializationContext);
        this.config = builder.config;
        this.format = builder.format;
        this.windowSize = builder.windowSize;
        this.packetSize = builder.packetSize;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public long getPacketSize() {
        return packetSize;
    }

    public SerializationContext<T> getSerializationContext() {
        return serializationContext;
    }

    public void writeTo(TReqWriteTableFragment.Builder builder) {
        builder.setSignedCookie(cookie.getPayload());
        if (config != null) {
            builder.setConfig(ByteString.copyFrom(config.toBinary()));
        }

        if (format != null) {
            builder.setFormat(ByteString.copyFrom(format.toBinary()));
        }
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public Builder<T> toBuilder() {
        return builder();
    }

    public static class Builder<T> extends BuilderBase<T, Builder<T>> {
        @Override
        protected Builder<T> self() {
            return this;
        }
    }

    public abstract static class BuilderBase<T,
            TBuilder extends BuilderBase<T, TBuilder>>
            extends RequestBase.Builder<TBuilder, WriteTableFragment<T>> {
        @Nullable
        private YTreeNode config;

        @Nullable
        private YTreeNode format;
        @Nullable
        private DistributedWriteCookie cookie;
        @Nullable
        private SerializationContext<T> serializationContext;

        private long windowSize = 16000000L;
        private long packetSize = windowSize / 2;

        public TBuilder setConfig(@Nullable YTreeNode config) {
            this.config = config;
            return self();
        }

        public TBuilder setFormat(@Nullable YTreeNode format) {
            this.format = format;
            return self();
        }

        public TBuilder setCookie(@Nullable DistributedWriteCookie cookie) {
            this.cookie = cookie;
            return self();
        }

        public TBuilder setSerializationContext(@Nullable SerializationContext<T> serializationContext) {
            this.serializationContext = serializationContext;
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

        @Override
        public WriteTableFragment<T> build() {
            return new WriteTableFragment<>(this);
        }
    }
}
