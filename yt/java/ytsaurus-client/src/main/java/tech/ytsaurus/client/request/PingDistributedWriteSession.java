package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqPingDistributedWriteSession;

/**
 * A request for ping a distributed write session.
 */
public class PingDistributedWriteSession
        extends RequestBase<PingDistributedWriteSession.Builder, PingDistributedWriteSession>
        implements HighLevelRequest<TReqPingDistributedWriteSession.Builder> {

    private final DistributedWriteSession session;

    public PingDistributedWriteSession(BuilderBase<?> builder) {
        super(builder);
        this.session = Objects.requireNonNull(builder.session);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPingDistributedWriteSession.Builder, ?> builder) {
        builder.body().setSignedSession(session.getPayload());
    }

    @Override
    public Builder toBuilder() {
        return builder().setSession(session);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected PingDistributedWriteSession.Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends PingDistributedWriteSession.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, PingDistributedWriteSession> {

        @Nullable
        private DistributedWriteSession session;

        public PingDistributedWriteSession build() {
            return new PingDistributedWriteSession(this);
        }

        public TBuilder setSession(DistributedWriteSession session) {
            this.session = session;
            return self();
        }
    }
}
