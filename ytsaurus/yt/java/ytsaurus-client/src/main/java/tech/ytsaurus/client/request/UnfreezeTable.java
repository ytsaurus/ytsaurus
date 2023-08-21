package tech.ytsaurus.client.request;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqUnfreezeTable;

public class UnfreezeTable
        extends TableReq<UnfreezeTable.Builder, UnfreezeTable>
        implements HighLevelRequest<TReqUnfreezeTable.Builder> {
    public UnfreezeTable(BuilderBase<?> builder) {
        super(builder);
    }

    public UnfreezeTable(YPath path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnfreezeTable.Builder, ?> builder) {
        super.writeTo(builder.body());
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setMutatingOptions(mutatingOptions)
                .setPath(path)
                .setTabletRangeOptions(tabletRangeOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends TableReq.Builder<TBuilder, UnfreezeTable> {
        @Override
        public UnfreezeTable build() {
            return new UnfreezeTable(this);
        }
    }
}
