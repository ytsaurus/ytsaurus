package ru.yandex.yt.ytclient.proxy;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import ru.yandex.inside.yt.kosher.common.YtTimestamp;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;

public class SelectRowsRequest extends RequestBase<SelectRowsRequest> {
    private final String query;
    private YtTimestamp timestamp;
    private Long inputRowsLimit;
    private Long outputRowsLimit;
    private Boolean failOnIncompleteResult;
    private Integer maxSubqueries;
    private Boolean allowJoinWithoutIndex;
    private String udfRegistryPath;
    private String executionPool;

    private SelectRowsRequest(String query) {
        this.query = query;
    }

    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest(query);
    }

    public String getQuery() {
        return query;
    }

    public SelectRowsRequest setTimestamp(YtTimestamp timestamp) {
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

    public void setMaxSubqueries(Integer maxSubqueries) {
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

    public SelectRowsRequest setExecutionPool(String executionPool) {
        this.executionPool = executionPool;
        return this;
    }

    public Optional<String> getExecutionPool() {
        return Optional.ofNullable(executionPool);
    }
}
