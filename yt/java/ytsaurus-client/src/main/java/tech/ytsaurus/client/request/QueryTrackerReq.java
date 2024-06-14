package tech.ytsaurus.client.request;

import java.util.function.Consumer;

abstract class QueryTrackerReq<
        TBuilder extends QueryTrackerReq.Builder<TBuilder, TRequest>,
        TRequest extends QueryTrackerReq<TBuilder, TRequest>>
        extends RequestBase<TBuilder, TRequest> {
    protected final String queryTrackerStage;

    protected QueryTrackerReq(Builder<?, ?> builder) {
        super(builder);
        this.queryTrackerStage = builder.queryTrackerStage;
    }

    protected void writeQueryTrackerDescriptionToProto(Consumer<String> queryTrackerStageSetter) {
        queryTrackerStageSetter.accept(queryTrackerStage);
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        sb.append("QueryTrackerStage: ").append(queryTrackerStage).append("; ");
    }

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends RequestBase<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
        protected String queryTrackerStage = "production";

        Builder() {
        }

        /**
         * Set Query tracker's stage in which queries will be run.
         * Default value is production.
         * @see QueryEngine
         * @return self
         */
        public TBuilder setQueryTrackerStage(String queryTrackerStage) {
            this.queryTrackerStage = queryTrackerStage;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            sb.append("QueryTrackerStage: ").append(queryTrackerStage).append("; ");
        }
    }
}
