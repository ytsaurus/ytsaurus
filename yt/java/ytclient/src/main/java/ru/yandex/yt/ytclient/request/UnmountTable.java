package ru.yandex.yt.ytclient.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqUnmountTable;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

/**
 * Unmount table request.
 *
 * @see <a href="https://yt.yandex-team.ru/docs/api/commands#unmount_table">documentation</a>
 * @see ru.yandex.yt.ytclient.proxy.ApiServiceClient#unmountTable(UnmountTable)
 */
@NonNullApi
public class UnmountTable
        extends TableReq<UnmountTable.Builder, UnmountTable>
        implements HighLevelRequest<TReqUnmountTable.Builder> {
    private final boolean force;

    public UnmountTable(BuilderBase<?> builder) {
        super(builder);
        this.force = builder.force;
    }

    public UnmountTable(YPath path) {
        this(builder().setPath(path.justPath()));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqUnmountTable.Builder, ?> builder) {
        super.writeTo(builder.body());
        builder.body().setForce(force);
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setForce(force)
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
            extends TableReq.Builder<TBuilder, UnmountTable> {
        private boolean force = false;

        protected BuilderBase() {
        }

        protected BuilderBase(BuilderBase<?> builder) {
            super(builder);
            this.force = builder.force;
        }

        /**
         * Force unmount.
         *
         * <b>Dangerous:</b> this flag should not be used unless you understand how it works.
         * Might lead to data loss.
         */
        public TBuilder setForce(boolean force) {
            this.force = force;
            return self();
        }

        @Override
        public UnmountTable build() {
            return new UnmountTable(this);
        }
    }
}
