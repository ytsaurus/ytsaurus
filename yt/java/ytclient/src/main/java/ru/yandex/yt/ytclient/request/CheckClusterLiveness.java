package ru.yandex.yt.ytclient.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.TReqCheckClusterLiveness;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
@NonNullFields
public class CheckClusterLiveness extends RequestBase<CheckClusterLiveness.Builder, CheckClusterLiveness>
        implements HighLevelRequest<TReqCheckClusterLiveness.Builder> {
    private final boolean checkCypressRoot;
    private final boolean checkSecondaryMasterCells;

    CheckClusterLiveness(Builder builder) {
        super(builder);
        this.checkCypressRoot = builder.checkCypressRoot;
        this.checkSecondaryMasterCells = builder.checkSecondaryMasterCells;
    }

    public CheckClusterLiveness() {
        this(builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqCheckClusterLiveness.Builder, ?> builder) {
        builder.body().setCheckCypressRoot(checkCypressRoot);
        builder.body().setCheckSecondaryMasterCells(checkSecondaryMasterCells);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setCheckCypressRoot(checkCypressRoot)
                .setCheckSecondaryMasterCells(checkSecondaryMasterCells)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends RequestBase.Builder<Builder, CheckClusterLiveness> {
        private boolean checkCypressRoot = false;
        private boolean checkSecondaryMasterCells = false;

        Builder() {
        }

        Builder(Builder builder) {
            super(builder);
            checkCypressRoot = builder.checkCypressRoot;
            checkSecondaryMasterCells = builder.checkSecondaryMasterCells;
        }

        /*
         * Checks cypress root availability.
         */
        public Builder setCheckCypressRoot(boolean checkCypressRoot) {
            this.checkCypressRoot = checkCypressRoot;
            return self();
        }

        /*
         * Checks secondary master cells generic availability.
         */
        public Builder setCheckSecondaryMasterCells(boolean checkSecondaryMasterCells) {
            this.checkSecondaryMasterCells = checkSecondaryMasterCells;
            return self();
        }

        public CheckClusterLiveness build() {
            return new CheckClusterLiveness(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
