package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class FreezeTable
        extends TableReq<FreezeTable.Builder, FreezeTable>
        implements HighLevelRequest<TReqFreezeTable.Builder> {
    public FreezeTable(BuilderBase<?> builder) {
        super(builder);
    }

    public FreezeTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqFreezeTable.Builder, ?> builder) {
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
            extends TableReq.Builder<TBuilder, FreezeTable> {
        @Override
        public FreezeTable build() {
            return new FreezeTable(this);
        }
    }
}


