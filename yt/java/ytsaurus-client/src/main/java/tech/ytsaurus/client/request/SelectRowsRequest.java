package tech.ytsaurus.client.request;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpcproxy.TReqSelectRows;

public class SelectRowsRequest
        extends RequestBase<SelectRowsRequest.Builder, SelectRowsRequest>
        implements HighLevelRequest<TReqSelectRows.Builder> {
    private final String query;
    @Nullable
    private final YtTimestamp timestamp;
    @Nullable
    private final YtTimestamp retentionTimestamp;
    @Nullable
    private final Long inputRowsLimit;
    @Nullable
    private final Long outputRowsLimit;
    @Nullable
    private final Boolean failOnIncompleteResult;
    @Nullable
    private final Integer maxSubqueries;
    @Nullable
    private final Boolean allowJoinWithoutIndex;
    @Nullable
    private final String udfRegistryPath;
    @Nullable
    private final String executionPool;
    @Nullable
    private final Boolean allowFullScan;
    @Nullable
    private final Long rangeExpansionLimit;
    @Nullable
    private final Boolean useCanonicalNullRelations;
    @Nullable
    private final ReplicaConsistency replicaConsistency;

    public SelectRowsRequest(BuilderBase<?> builder) {
        super(builder);
        this.query = Objects.requireNonNull(builder.query);
        this.timestamp = builder.timestamp;
        this.retentionTimestamp = builder.retentionTimestamp;
        this.inputRowsLimit = builder.inputRowsLimit;
        this.outputRowsLimit = builder.outputRowsLimit;
        this.failOnIncompleteResult = builder.failOnIncompleteResult;
        this.maxSubqueries = builder.maxSubqueries;
        this.allowJoinWithoutIndex = builder.allowJoinWithoutIndex;
        this.udfRegistryPath = builder.udfRegistryPath;
        this.executionPool = builder.executionPool;
        this.allowFullScan = builder.allowFullScan;
        this.rangeExpansionLimit = builder.rangeExpansionLimit;
        this.useCanonicalNullRelations = builder.useCanonicalNullRelations;
        this.replicaConsistency = builder.replicaConsistency;
    }

    private SelectRowsRequest(String query) {
        this(builder().setQuery(query));
    }

    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest(query);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getQuery() {
        return query;
    }

    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    public Optional<YtTimestamp> getRetentionTimestamp() {
        return Optional.ofNullable(retentionTimestamp);
    }

    public OptionalLong getInputRowsLimit() {
        return inputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(inputRowsLimit);
    }

    public Optional<Boolean> getFailOnIncompleteResult() {
        return Optional.ofNullable(failOnIncompleteResult);
    }

    public OptionalLong getOutputRowsLimit() {
        return outputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(outputRowsLimit);
    }

    public OptionalInt getMaxSubqueries() {
        return maxSubqueries == null ? OptionalInt.empty() : OptionalInt.of(maxSubqueries);
    }

    public Optional<Boolean> getAllowJoinWithoutIndex() {
        return Optional.ofNullable(allowJoinWithoutIndex);
    }

    public Optional<String> getUdfRegistryPath() {
        return Optional.ofNullable(udfRegistryPath);
    }

    public Optional<String> getExecutionPool() {
        return Optional.ofNullable(executionPool);
    }

    public Optional<Boolean> getAllowFullScan() {
        return Optional.ofNullable(allowFullScan);
    }

    public Optional<Long> getRangeExpansionLimit() {
        return Optional.ofNullable(rangeExpansionLimit);
    }

    public Optional<Boolean> getUseCanonicalNullRelations() {
        return Optional.ofNullable(useCanonicalNullRelations);
    }

    public Optional<ReplicaConsistency> getReplicaConsistency() {
        return Optional.ofNullable(replicaConsistency);
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSelectRows.Builder, ?> builder) {
        builder.body().setQuery(getQuery());
        if (getTimestamp().isPresent()) {
            builder.body().setTimestamp(getTimestamp().get().getValue());
        }
        if (getRetentionTimestamp().isPresent()) {
            builder.body().setRetentionTimestamp(getRetentionTimestamp().get().getValue());
        }
        if (getInputRowsLimit().isPresent()) {
            builder.body().setInputRowLimit(getInputRowsLimit().getAsLong());
        }
        if (getOutputRowsLimit().isPresent()) {
            builder.body().setOutputRowLimit(getOutputRowsLimit().getAsLong());
        }
        if (getFailOnIncompleteResult().isPresent()) {
            builder.body().setFailOnIncompleteResult(getFailOnIncompleteResult().get());
        }
        if (getMaxSubqueries().isPresent()) {
            builder.body().setMaxSubqueries(getMaxSubqueries().getAsInt());
        }
        if (getAllowJoinWithoutIndex().isPresent()) {
            builder.body().setAllowJoinWithoutIndex(getAllowJoinWithoutIndex().get());
        }
        if (getUdfRegistryPath().isPresent()) {
            builder.body().setUdfRegistryPath(getUdfRegistryPath().get());
        }
        if (getExecutionPool().isPresent()) {
            builder.body().setExecutionPool(getExecutionPool().get());
        }
        if (getAllowFullScan().isPresent()) {
            builder.body().setAllowFullScan(getAllowFullScan().get());
        }
        if (getRangeExpansionLimit().isPresent()) {
            builder.body().setRangeExpansionLimit(getRangeExpansionLimit().get());
        }
        if (getUseCanonicalNullRelations().isPresent()) {
            builder.body().setUseCanonicalNullRelations(getUseCanonicalNullRelations().get());
        }
        if (getReplicaConsistency().isPresent()) {
            builder.body().setReplicaConsistency(getReplicaConsistency().get().getProtoValue());
        }
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Query: ").append(query).append("; ");
    }

    @Override
    public Builder toBuilder() {
        Builder builder = builder()
                .setQuery(query)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
        if (timestamp != null) {
            builder.setTimestamp(timestamp);
        }
        if (retentionTimestamp != null) {
            builder.setRetentionTimestamp(retentionTimestamp);
        }
        if (inputRowsLimit != null) {
            builder.setInputRowsLimit(inputRowsLimit);
        }
        if (outputRowsLimit != null) {
            builder.setOutputRowsLimit(outputRowsLimit);
        }
        if (failOnIncompleteResult != null) {
            builder.setFailOnIncompleteResult(failOnIncompleteResult);
        }
        if (maxSubqueries != null) {
            builder.setMaxSubqueries(maxSubqueries);
        }
        if (allowJoinWithoutIndex != null) {
            builder.setAllowJoinWithoutIndex(allowJoinWithoutIndex);
        }
        if (udfRegistryPath != null) {
            builder.setUdfRegistryPath(udfRegistryPath);
        }
        if (executionPool != null) {
            builder.setExecutionPool(executionPool);
        }
        if (allowFullScan != null) {
            builder.setAllowFullScan(allowFullScan);
        }
        if (rangeExpansionLimit != null) {
            builder.setRangeExpansionLimit(rangeExpansionLimit);
        }
        if (useCanonicalNullRelations != null) {
            builder.setUseCanonicalNullRelations(useCanonicalNullRelations);
        }
        if (replicaConsistency != null) {
            builder.setReplicaConsistency(replicaConsistency);
        }
        return builder;
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends RequestBase.Builder<TBuilder, SelectRowsRequest> {
        @Nullable
        private String query;
        @Nullable
        private YtTimestamp timestamp;
        @Nullable
        private YtTimestamp retentionTimestamp;
        @Nullable
        private Long inputRowsLimit;
        @Nullable
        private Long outputRowsLimit;
        @Nullable
        private Boolean failOnIncompleteResult;
        @Nullable
        private Integer maxSubqueries;
        @Nullable
        private Boolean allowJoinWithoutIndex;
        @Nullable
        private String udfRegistryPath;
        @Nullable
        private String executionPool;
        @Nullable
        private Boolean allowFullScan;
        @Nullable
        private Long rangeExpansionLimit;
        @Nullable
        private Boolean useCanonicalNullRelations;
        @Nullable
        private ReplicaConsistency replicaConsistency;

        public BuilderBase() {
        }

        BuilderBase(BuilderBase<?> builder) {
            super(builder);
            query = builder.query;
            timestamp = builder.timestamp;
            retentionTimestamp = builder.retentionTimestamp;
            inputRowsLimit = builder.inputRowsLimit;
            outputRowsLimit = builder.outputRowsLimit;
            failOnIncompleteResult = builder.failOnIncompleteResult;
            maxSubqueries = builder.maxSubqueries;
            allowJoinWithoutIndex = builder.allowJoinWithoutIndex;
            udfRegistryPath = builder.udfRegistryPath;
            executionPool = builder.executionPool;
            allowFullScan = builder.allowFullScan;
            rangeExpansionLimit = builder.rangeExpansionLimit;
            useCanonicalNullRelations = builder.useCanonicalNullRelations;
            replicaConsistency = builder.replicaConsistency;
        }

        public TBuilder setQuery(String query) {
            this.query = query;
            return self();
        }

        public TBuilder setTimestamp(YtTimestamp timestamp) {
            this.timestamp = timestamp;
            return self();
        }

        public TBuilder setRetentionTimestamp(YtTimestamp retentionTimestamp) {
            this.retentionTimestamp = retentionTimestamp;
            return self();
        }

        public TBuilder setInputRowsLimit(long inputRowsLimit) {
            this.inputRowsLimit = inputRowsLimit;
            return self();
        }

        public TBuilder setOutputRowsLimit(long outputRowsLimit) {
            this.outputRowsLimit = outputRowsLimit;
            return self();
        }

        public TBuilder setFailOnIncompleteResult(boolean failOnIncompleteResult) {
            this.failOnIncompleteResult = failOnIncompleteResult;
            return self();
        }

        public TBuilder setMaxSubqueries(int maxSubqueries) {
            this.maxSubqueries = maxSubqueries;
            return self();
        }

        public TBuilder setAllowJoinWithoutIndex(boolean allowJoinWithoutIndex) {
            this.allowJoinWithoutIndex = allowJoinWithoutIndex;
            return self();
        }

        public TBuilder setUdfRegistryPath(String udfRegistryPath) {
            this.udfRegistryPath = udfRegistryPath;
            return self();
        }

        public TBuilder setExecutionPool(String executionPool) {
            this.executionPool = executionPool;
            return self();
        }

        public TBuilder setAllowFullScan(boolean allowFullScan) {
            this.allowFullScan = allowFullScan;
            return self();
        }

        public TBuilder setRangeExpansionLimit(long rangeExpansionLimit) {
            this.rangeExpansionLimit = rangeExpansionLimit;
            return self();
        }

        public TBuilder setUseCanonicalNullRelations(boolean useCanonicalNullRelations) {
            this.useCanonicalNullRelations = useCanonicalNullRelations;
            return self();
        }

        public TBuilder setReplicaConsistency(ReplicaConsistency replicaConsistency) {
            this.replicaConsistency = replicaConsistency;
            return self();
        }

        public String getQuery() {
            return Objects.requireNonNull(query);
        }

        public Optional<YtTimestamp> getTimestamp() {
            return Optional.ofNullable(timestamp);
        }

        public Optional<YtTimestamp> getRetentionTimestamp() {
            return Optional.ofNullable(retentionTimestamp);
        }

        public OptionalLong getInputRowsLimit() {
            return inputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(inputRowsLimit);
        }

        public Optional<Boolean> getFailOnIncompleteResult() {
            return Optional.ofNullable(failOnIncompleteResult);
        }

        public OptionalLong getOutputRowsLimit() {
            return outputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(outputRowsLimit);
        }

        public OptionalInt getMaxSubqueries() {
            return maxSubqueries == null ? OptionalInt.empty() : OptionalInt.of(maxSubqueries);
        }

        public Optional<Boolean> getAllowJoinWithoutIndex() {
            return Optional.ofNullable(allowJoinWithoutIndex);
        }

        public Optional<String> getUdfRegistryPath() {
            return Optional.ofNullable(udfRegistryPath);
        }

        public Optional<String> getExecutionPool() {
            return Optional.ofNullable(executionPool);
        }

        public Optional<Boolean> getAllowFullScan() {
            return Optional.ofNullable(allowFullScan);
        }

        public Optional<Long> getRangeExpansionLimit() {
            return Optional.ofNullable(rangeExpansionLimit);
        }

        public Optional<Boolean> getUseCanonicalNullRelations() {
            return Optional.ofNullable(useCanonicalNullRelations);
        }

        public Optional<ReplicaConsistency> getReplicaConsistency() {
            return Optional.ofNullable(replicaConsistency);
        }

        @Override
        protected void writeArgumentsLogString(StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            sb.append("Query: ").append(query).append("; ");
        }

        @Override
        public SelectRowsRequest build() {
            return new SelectRowsRequest(this);
        }
    }
}
