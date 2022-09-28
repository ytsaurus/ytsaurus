package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqFreezeTable;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class FreezeTable
        extends TableReq<FreezeTable.Builder, FreezeTable>
        implements HighLevelRequest<TReqFreezeTable.Builder> {
    public FreezeTable(BuilderBase<?, ?> builder) {
        super(builder);
    }

    public FreezeTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

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

    public static class Builder extends BuilderBase<Builder, FreezeTable> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public FreezeTable build() {
            return new FreezeTable(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends TableReq<?, TRequest>>
            extends TableReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqFreezeTable.Builder> {
        @Override
        public void writeTo(RpcClientRequestBuilder<TReqFreezeTable.Builder, ?> builder) {
            super.writeTo(builder.body());
        }
    }
}


