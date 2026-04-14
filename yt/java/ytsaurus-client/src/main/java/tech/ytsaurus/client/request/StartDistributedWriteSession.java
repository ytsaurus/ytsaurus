package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqStartDistributedWriteSession;

/**
 * Distributed write session start request. Must contain a path to write and a number of cookies to use.
 */
public class StartDistributedWriteSession
        extends RequestBase<StartDistributedWriteSession.Builder, StartDistributedWriteSession>
        implements HighLevelRequest<TReqStartDistributedWriteSession.Builder> {

    private final YPath path;
    private final int cookieCount;
    @Nullable
    private final Long writeTimeout;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    @Nullable
    private final Duration pingPeriod;
    @Nullable
    private final Duration failedPingRetryPeriod;
    @Nullable
    private final Consumer<Exception> onPingFailed;

    public StartDistributedWriteSession(BuilderBase<?> builder) {
        super(builder);
        this.path =  Objects.requireNonNull(builder.path);
        this.cookieCount = Objects.requireNonNull(builder.cookieCount);
        this.writeTimeout = builder.writeTimeout;
        this.transactionalOptions = builder.transactionalOptions;

        this.pingPeriod = builder.pingPeriod;
        this.failedPingRetryPeriod = builder.failedPingRetryPeriod;
        this.onPingFailed = builder.onPingFailed;
    }

    public YPath getPath() {
        return path;
    }

    public int getCookieCount() {
        return cookieCount;
    }

    @Nullable
    public Long getWriteTimeout() {
        return writeTimeout;
    }

    @Nullable
    public TransactionalOptions getTransactionalOptions() {
        return transactionalOptions;
    }

    @Nullable
    public Duration getPingPeriod() {
        return pingPeriod;
    }

    @Nullable
    public Duration getFailedPingRetryPeriod() {
        return failedPingRetryPeriod;
    }

    @Nullable
    public Consumer<Exception> getOnPingFailed() {
        return onPingFailed;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqStartDistributedWriteSession.Builder, ?> builder) {
        builder.body()
                .setPath(ByteString.copyFromUtf8(path.toString()))
                .setCookieCount(cookieCount);

        if (writeTimeout != null) {
            builder.body().setSessionTimeout(writeTimeout);
        }

        if (transactionalOptions != null) {
            builder.body().setTransactionalOptions(transactionalOptions.toProto());
        }

    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPath(path)
                .setCookieCount(cookieCount)
                .setWriteTimeout(writeTimeout)
                .setTransactionalOptions(transactionalOptions);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected StartDistributedWriteSession.Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends StartDistributedWriteSession.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, StartDistributedWriteSession> {

        @Nullable
        private YPath path;
        @Nullable
        private Integer cookieCount;
        @Nullable
        private Long writeTimeout;
        @Nullable
        private TransactionalOptions transactionalOptions;

        @Nullable
        private Duration pingPeriod = Duration.ofSeconds(5);
        @Nullable
        private Duration failedPingRetryPeriod;
        @Nullable
        private Consumer<Exception> onPingFailed;


        public StartDistributedWriteSession build() {
            return new StartDistributedWriteSession(this);
        }

        public TBuilder setPath(@Nullable YPath path) {
            this.path = path;
            return self();
        }

        public TBuilder setCookieCount(int cookieCount) {
            this.cookieCount = cookieCount;
            return self();
        }

        public TBuilder setWriteTimeout(@Nullable Long writeTimeout) {
            this.writeTimeout = writeTimeout;
            return self();
        }

        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        public TBuilder setPingPeriod(@Nullable Duration pingPeriod) {
            this.pingPeriod = pingPeriod;
            return self();
        }

        public TBuilder setFailedPingRetryPeriod(@Nullable Duration failedPingRetryPeriod) {
            this.failedPingRetryPeriod = failedPingRetryPeriod;
            return self();
        }

        public TBuilder setOnPingFailed(@Nullable Consumer<Exception> onPingFailed) {
            this.onPingFailed = onPingFailed;
            return self();
        }
    }
}
