package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqRemountTable;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class RemountTable
        extends TableReq<RemountTable.Builder, RemountTable>
        implements HighLevelRequest<TReqRemountTable.Builder> {
    public RemountTable(BuilderBase<?, ?> builder) {
        super(builder);
    }

    public RemountTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqRemountTable.Builder, ?> builder) {
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

    public static class Builder extends BuilderBase<Builder, RemountTable> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public RemountTable build() {
            return new RemountTable(this);
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder, TRequest>,
            TRequest extends TableReq<?, TRequest>>
            extends TableReq.Builder<TBuilder, TRequest>
            implements HighLevelRequest<TReqRemountTable.Builder> {
        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?, ?> builder) {
            super(builder);
        }

        @Override
        public void writeTo(RpcClientRequestBuilder<TReqRemountTable.Builder, ?> builder) {
            super.writeTo(builder.body());
        }
    }
}
