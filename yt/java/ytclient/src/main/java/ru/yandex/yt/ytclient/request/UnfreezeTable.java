package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqUnfreezeTable;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class UnfreezeTable
        extends TableReq<UnfreezeTable.Builder, UnfreezeTable>
        implements HighLevelRequest<TReqUnfreezeTable.Builder> {
    public UnfreezeTable(BuilderBase<?, ?> builder) {
        super(builder);
    }

    public UnfreezeTable(YPath path) {
        this(builder().setPath(path));
    }

    public static Builder builder() {
        return new Builder();
    }

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

    public static class Builder extends BuilderBase<Builder, UnfreezeTable> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public UnfreezeTable build() {
            return new UnfreezeTable(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends TableReq<?, TRequest>>
            extends TableReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqUnfreezeTable.Builder> {
        @Override
        public void writeTo(RpcClientRequestBuilder<TReqUnfreezeTable.Builder, ?> builder) {
            super.writeTo(builder.body());
        }
    }
}
