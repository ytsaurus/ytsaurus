package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.rpcproxy.TReqFinishDistributedWriteSession;

/**
 * A request to finish distributed write session. It must contain WriteFragmentResult from all WriteTableFragment
 * invocations.
 */
public class FinishDistributedWriteSession
        extends RequestBase<FinishDistributedWriteSession.Builder, FinishDistributedWriteSession>
        implements HighLevelRequest<TReqFinishDistributedWriteSession.Builder> {

    private final DistributedWriteSession session;
    private final List<WriteFragmentResult> writeResults;

    public FinishDistributedWriteSession(BuilderBase<?> builder) {
        super(builder);
        this.session = Objects.requireNonNull(builder.session);
        this.writeResults = Objects.requireNonNull(builder.writeResults);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqFinishDistributedWriteSession.Builder, ?> builder) {
        builder.body()
                .setSignedSession(session.getPayload())
                .addAllSignedWriteResults(
                        writeResults.stream().map(WriteFragmentResult::getPayload).collect(Collectors.toList())
                )
                .setMaxChildrenPerAttachRequest(0); // TODO remove after YT-26131
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setSession(session)
                .setWriteResults(writeResults);
    }

    public static class Builder extends BuilderBase<FinishDistributedWriteSession.Builder> {
        @Override
        protected FinishDistributedWriteSession.Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends FinishDistributedWriteSession.BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, FinishDistributedWriteSession> {
        @Nullable
        private DistributedWriteSession session;
        @Nullable
        private List<WriteFragmentResult> writeResults;

        public TBuilder setSession(@Nullable DistributedWriteSession session) {
            this.session = session;
            return self();
        }

        public TBuilder setWriteResults(@Nullable List<WriteFragmentResult> writeResults) {
            this.writeResults = writeResults;
            return self();
        }

        @Override
        public FinishDistributedWriteSession build() {
            return new FinishDistributedWriteSession(this);
        }
    }
}
