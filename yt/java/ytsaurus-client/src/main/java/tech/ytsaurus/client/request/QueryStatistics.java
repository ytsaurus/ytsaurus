package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TQueryStatistics;
import tech.ytsaurus.rpcproxy.TQueryStatistics.TAggregate;

class LongAggregate {
    @Nullable
    private final Long total;
    @Nullable
    private final Long max;
    @Nullable
    private final String argmaxNode;

    LongAggregate(Long value) {
        this.total = value;
        this.max = value;
        this.argmaxNode = null;
    }

    LongAggregate(TAggregate aggregate) {
        this.total = aggregate.hasTotal() ? aggregate.getTotal() : null;
        this.max = aggregate.hasMax() ? aggregate.getTotal() : null;
        this.argmaxNode = aggregate.hasArgmaxNode() ? aggregate.getArgmaxNode() : null;
    }
}

class DurationAggregate {
    @Nullable
    private final Duration total;
    @Nullable
    private final Duration max;
    @Nullable
    private final String argmaxNode;

    DurationAggregate(Long value) {
        this.total = RpcUtil.durationFromMicros(value);
        this.max = RpcUtil.durationFromMicros(value);
        this.argmaxNode = null;
    }

    DurationAggregate(TAggregate aggregate) {
        this.total = aggregate.hasTotal()
                ? RpcUtil.durationFromMicros(aggregate.getTotal())
                : null;
        this.max = aggregate.hasMax()
                ? RpcUtil.durationFromMicros(aggregate.getTotal())
                : null;
        this.argmaxNode = aggregate.hasArgmaxNode()
                ? aggregate.getArgmaxNode()
                : null;
    }
}

/**
 * Immutable metadata about query statistics.
 *
 * @see tech.ytsaurus.client.SelectRowsResult#getQueryStatistics()
 */
public class QueryStatistics {
    @Nullable
    private final LongAggregate rowsRead;
    @Nullable
    private final LongAggregate dataWeightRead;
    @Nullable
    private final LongAggregate rowsWritten;
    @Nullable
    private final DurationAggregate syncTime;
    @Nullable
    private final DurationAggregate asyncTime;
    @Nullable
    private final DurationAggregate executeTime;
    @Nullable
    private final DurationAggregate readTime;
    @Nullable
    private final DurationAggregate writeTime;
    @Nullable
    private final DurationAggregate codegenTime;
    @Nullable
    private final DurationAggregate waitOnReadyEventTime;
    private final boolean incompleteInput;
    private final boolean incompleteOutput;
    @Nullable
    private final LongAggregate memoryUsage;
    @Nullable
    private final LongAggregate groupedRowCount;
    @Nullable
    private final Long queryCount;
    private final List<QueryStatistics> innerStatistics;

    public QueryStatistics(TQueryStatistics stats) {
        this.rowsRead = stats.hasRowsReadAggr()
                ? new LongAggregate(stats.getRowsReadAggr())
                : stats.hasRowsRead()
                        ? new LongAggregate(stats.getRowsRead())
                        : null;
        this.dataWeightRead = stats.hasDataWeightReadAggr()
                ? new LongAggregate(stats.getDataWeightReadAggr())
                : stats.hasDataWeightRead()
                        ? new LongAggregate(stats.getDataWeightRead())
                        : null;
        this.rowsWritten = stats.hasRowsWrittenAggr()
                ? new LongAggregate(stats.getRowsWrittenAggr())
                : stats.hasRowsWritten()
                        ? new LongAggregate(stats.getRowsWritten())
                        : null;
        this.syncTime = stats.hasSyncTimeAggr()
                ? new DurationAggregate(stats.getSyncTimeAggr())
                : stats.hasSyncTime()
                        ? new DurationAggregate(stats.getSyncTime())
                        : null;
        this.asyncTime = stats.hasAsyncTimeAggr()
                ? new DurationAggregate(stats.getAsyncTimeAggr())
                : stats.hasAsyncTime()
                        ? new DurationAggregate(stats.getAsyncTime())
                        : null;
        this.executeTime = stats.hasExecuteTimeAggr()
                ? new DurationAggregate(stats.getExecuteTimeAggr())
                : stats.hasExecuteTime()
                        ? new DurationAggregate(stats.getExecuteTime())
                        : null;
        this.readTime = stats.hasReadTimeAggr()
                ? new DurationAggregate(stats.getReadTimeAggr())
                : stats.hasReadTime()
                        ? new DurationAggregate(stats.getReadTime())
                        : null;
        this.writeTime = stats.hasWriteTimeAggr()
                ? new DurationAggregate(stats.getWriteTimeAggr())
                : stats.hasWriteTime()
                        ? new DurationAggregate(stats.getWriteTime())
                        : null;
        this.codegenTime = stats.hasCodegenTimeAggr()
                ? new DurationAggregate(stats.getCodegenTimeAggr())
                : stats.hasCodegenTime()
                        ? new DurationAggregate(stats.getCodegenTime())
                        : null;
        this.waitOnReadyEventTime = stats.hasWaitOnReadyEventTimeAggr()
                ? new DurationAggregate(stats.getWaitOnReadyEventTimeAggr())
                : stats.hasWaitOnReadyEventTime()
                        ? new DurationAggregate(stats.getWaitOnReadyEventTime())
                        : null;
        this.memoryUsage = stats.hasMemoryUsageAggr()
                ? new LongAggregate(stats.getMemoryUsageAggr())
                : stats.hasMemoryUsage()
                        ? new LongAggregate(stats.getMemoryUsage())
                        : null;
        this.groupedRowCount = stats.hasGroupedRowCountAggr()
                ? new LongAggregate(stats.getGroupedRowCountAggr())
                : stats.hasGroupedRowCount()
                        ? new LongAggregate(stats.getGroupedRowCount())
                        : null;

        this.incompleteInput = stats.getIncompleteInput();
        this.incompleteOutput = stats.getIncompleteOutput();
        this.queryCount = stats.hasQueryCount() ? stats.getQueryCount() : null;

        this.innerStatistics = stats.getInnerStatisticsList().stream()
                .map(QueryStatistics::new)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Get the number of rows read aggregate if present.
     *
     * @return number of rows read aggregate if present.
     */
    public Optional<LongAggregate> getRowsRead() {
        return Optional.ofNullable(rowsRead);
    }

    /**
     * Get the weight of the read data aggregate if present.
     *
     * @return weight of the read data aggregate if present.
     */
    public Optional<LongAggregate> getDataWeightRead() {
        return Optional.ofNullable(dataWeightRead);
    }

    /**
     * Get the number of rows written aggregate if present.
     *
     * @return number of rows written aggregate if present.
     */
    public Optional<LongAggregate> getRowsWritten() {
        return Optional.ofNullable(rowsWritten);
    }

    /**
     * Get the sync time aggregate if present.
     *
     * @return sync time aggregate if present.
     */
    public Optional<DurationAggregate> getSyncTime() {
        return Optional.ofNullable(syncTime);
    }

    /**
     * Get the async time aggregate if present.
     *
     * @return async time aggregate if present.
     */
    public Optional<DurationAggregate> getAsyncTime() {
        return Optional.ofNullable(asyncTime);
    }

    /**
     * Get the execution time aggregate if present.
     *
     * @return execution time aggregate if present.
     */
    public Optional<DurationAggregate> getExecuteTime() {
        return Optional.ofNullable(executeTime);
    }

    /**
     * Get the read time aggregate if present.
     *
     * @return read time aggregate if present.
     */
    public Optional<DurationAggregate> getReadTime() {
        return Optional.ofNullable(readTime);
    }

    /**
     * Get the write time aggregate if present.
     *
     * @return write time aggregate if present.
     */
    public Optional<DurationAggregate> getWriteTime() {
        return Optional.ofNullable(writeTime);
    }

    /**
     * Get the codegen time aggregate if present.
     *
     * @return codegen time aggregate if present.
     */
    public Optional<DurationAggregate> getCodegenTime() {
        return Optional.ofNullable(codegenTime);
    }

    /**
     * Get the wait on ready event time aggregate if present.
     *
     * @return wait on ready event time aggregate if present.
     */
    public Optional<DurationAggregate> getWaitOnReadyEventTime() {
        return Optional.ofNullable(waitOnReadyEventTime);
    }

    /**
     * Checks if the input is incomplete.
     *
     * @return {@code true} if the input is incomplete; {@code false} otherwise.
     */
    public boolean isInputIncomplete() {
        return incompleteInput;
    }

    /**
     * Checks if the output is incomplete.
     *
     * @return {@code true} if the output is incomplete; {@code false} otherwise.
     */
    public boolean isOutputIncomplete() {
        return incompleteOutput;
    }

    /**
     * Get the memory usage aggregate if present.
     *
     * @return memory usage aggregate if present.
     */
    public Optional<LongAggregate> getMemoryUsage() {
        return Optional.ofNullable(memoryUsage);
    }

    /**
     * Get the grouped row count aggregate if present.
     *
     * @return grouped row count aggregate if present.
     */
    public Optional<LongAggregate> getGroupedRowCount() {
        return Optional.ofNullable(groupedRowCount);
    }

    /**
     * Get the query count if present.
     *
     * @return query count if present.
     */
    public Optional<Long> getQueryCount() {
        return Optional.ofNullable(queryCount);
    }

    /**
     * Get inner statistics.
     *
     * @return inner statistics.
     */
    public List<QueryStatistics> getInnerStatistics() {
        return innerStatistics;
    }
}
