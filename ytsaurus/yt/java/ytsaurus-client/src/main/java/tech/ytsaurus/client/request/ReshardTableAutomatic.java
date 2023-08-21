package tech.ytsaurus.client.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqReshardTableAutomatic;

public class ReshardTableAutomatic
        extends TableReq<ReshardTableAutomatic.Builder, ReshardTableAutomatic> {
    private final boolean keepActions;

    public ReshardTableAutomatic(BuilderBase<?> builder) {
        super(builder);
        this.keepActions = builder.keepActions;
    }

    public ReshardTableAutomatic(YPath path, boolean keepActions) {
        this(builder().setPath(path.justPath()).setKeepActions(keepActions));
    }

    public static Builder builder() {
        return new Builder();
    }

    public TReqReshardTableAutomatic.Builder writeTo(TReqReshardTableAutomatic.Builder builder) {
        super.writeTo(builder);
        builder.setKeepActions(keepActions);
        return builder;
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setKeepActions(keepActions)
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
            extends TableReq.Builder<TBuilder, ReshardTableAutomatic> {
        private boolean keepActions = false;

        public TBuilder setKeepActions(boolean keepActions) {
            this.keepActions = keepActions;
            return self();
        }

        public TReqReshardTableAutomatic.Builder writeTo(TReqReshardTableAutomatic.Builder builder) {
            super.writeTo(builder);
            builder.setKeepActions(keepActions);
            return builder;
        }

        @Override
        public ReshardTableAutomatic build() {
            return new ReshardTableAutomatic(this);
        }
    }
}
