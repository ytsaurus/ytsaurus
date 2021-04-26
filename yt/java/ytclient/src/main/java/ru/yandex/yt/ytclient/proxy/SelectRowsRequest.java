package ru.yandex.yt.ytclient.proxy;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.rpcproxy.TReqSelectRows;
import ru.yandex.yt.ytclient.proxy.request.HighLevelRequest;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

@NonNullApi
public class SelectRowsRequest
        extends RequestBase<SelectRowsRequest>
        implements HighLevelRequest<TReqSelectRows.Builder> {
    private final String query;
    @Nullable private YtTimestamp timestamp;
    @Nullable private Long inputRowsLimit;
    @Nullable private Long outputRowsLimit;
    @Nullable private Boolean failOnIncompleteResult;
    @Nullable private Integer maxSubqueries;
    @Nullable private Boolean allowJoinWithoutIndex;
    @Nullable private String udfRegistryPath;
    @Nullable private String executionPool;

    private SelectRowsRequest(String query) {
        this.query = query;
    }

    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest(query);
    }

    public String getQuery() {
        return query;
    }

    public SelectRowsRequest setTimestamp(@Nullable YtTimestamp timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Optional<YtTimestamp> getTimestamp() {
        return Optional.ofNullable(timestamp);
    }

    public SelectRowsRequest setInputRowsLimit(long inputRowsLimit) {
        this.inputRowsLimit = inputRowsLimit;
        return this;
    }

    public OptionalLong getInputRowsLimit() {
        return inputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(inputRowsLimit);
    }

    public SelectRowsRequest setFailOnIncompleteResult(boolean failOnIncompleteResult) {
        this.failOnIncompleteResult = failOnIncompleteResult;
        return this;
    }

    public Optional<Boolean> getFailOnIncompleteResult() {
        return Optional.ofNullable(failOnIncompleteResult);
    }

    public SelectRowsRequest setOutputRowsLimit(long outputRowsLimit) {
        this.outputRowsLimit = outputRowsLimit;
        return this;
    }

    public OptionalLong getOutputRowsLimit() {
        return outputRowsLimit == null ? OptionalLong.empty() : OptionalLong.of(outputRowsLimit);
    }

    public void setMaxSubqueries(@Nullable Integer maxSubqueries) {
        this.maxSubqueries = maxSubqueries;
    }

    public OptionalInt getMaxSubqueries() {
        return maxSubqueries == null ? OptionalInt.empty() : OptionalInt.of(maxSubqueries);
    }

    public SelectRowsRequest setAllowJoinWithoutIndex(boolean allowJoinWithoutIndex) {
        this.allowJoinWithoutIndex = allowJoinWithoutIndex;
        return this;
    }

    public Optional<Boolean> getAllowJoinWithoutIndex() {
        return Optional.ofNullable(allowJoinWithoutIndex);
    }

    public SelectRowsRequest setUdfRegistryPath(String udfRegistryPath) {
        this.udfRegistryPath = udfRegistryPath;
        return this;
    }

    public Optional<String> getUdfRegistryPath() {
        return Optional.ofNullable(udfRegistryPath);
    }

    public SelectRowsRequest setExecutionPool(@Nullable String executionPool) {
        this.executionPool = executionPool;
        return this;
    }

    public Optional<String> getExecutionPool() {
        return Optional.ofNullable(executionPool);
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqSelectRows.Builder, ?> builder) {
        builder.body().setQuery(getQuery());
        if (getTimestamp().isPresent()) {
            builder.body().setTimestamp(getTimestamp().get().getValue());
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
    }

    @Nonnull
    @Override
    protected SelectRowsRequest self() {
        return this;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Query: ").append(query).append("; ");
    }
}
