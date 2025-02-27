package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TQueryStatistics;

/**
 * Immutable metadata about query statistics.
 *
 * @see tech.ytsaurus.client.SelectRowsResult#getQueryStatistics()
 */
public class QueryStatistics {
    @Nullable
    private final Long rowsRead;
    @Nullable
    private final Long dataWeightRead;
    @Nullable
    private final Long rowsWritten;
    @Nullable
    private final Duration syncTime;
    @Nullable
    private final Duration asyncTime;
    @Nullable
    private final Duration executeTime;
    @Nullable
    private final Duration readTime;
    @Nullable
    private final Duration writeTime;
    @Nullable
    private final Duration codegenTime;
    @Nullable
    private final Duration waitOnReadyEventTime;
    private final boolean incompleteInput;
    private final boolean incompleteOutput;
    @Nullable
    private final Long memoryUsage;
    @Nullable
    private final Long totalGroupedRowCount;
    private final List<QueryStatistics> innerStatistics;

    public QueryStatistics(TQueryStatistics stats) {
        this.rowsRead = stats.hasRowsRead() ? stats.getRowsRead() : null;
        this.dataWeightRead = stats.hasDataWeightRead() ? stats.getDataWeightRead() : null;
        this.rowsWritten = stats.hasRowsWritten() ? stats.getRowsWritten() : null;
        this.syncTime = stats.hasSyncTime() ? RpcUtil.durationFromMicros(stats.getSyncTime()) : null;
        this.asyncTime = stats.hasAsyncTime() ? RpcUtil.durationFromMicros(stats.getAsyncTime()) : null;
        this.executeTime = stats.hasExecuteTime() ? RpcUtil.durationFromMicros(stats.getExecuteTime()) : null;
        this.readTime = stats.hasReadTime() ? RpcUtil.durationFromMicros(stats.getReadTime()) : null;
        this.writeTime = stats.hasWriteTime() ? RpcUtil.durationFromMicros(stats.getWriteTime()) : null;
        this.codegenTime = stats.hasCodegenTime() ? RpcUtil.durationFromMicros(stats.getCodegenTime()) : null;
        this.waitOnReadyEventTime = stats.hasWaitOnReadyEventTime()
                ? RpcUtil.durationFromMicros(stats.getWaitOnReadyEventTime())
                : null;
        this.incompleteInput = stats.getIncompleteInput();
        this.incompleteOutput = stats.getIncompleteOutput();
        this.memoryUsage = stats.hasMemoryUsage() ? stats.getMemoryUsage() : null;
        this.totalGroupedRowCount = stats.hasTotalGroupedRowCount() ? stats.getTotalGroupedRowCount() : null;
        this.innerStatistics = stats.getInnerStatisticsList().stream()
                .map(QueryStatistics::new)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Get the number of rows read if present.
     *
     * @return number of rows read if present.
     */
    public Optional<Long> getRowsRead() {
        return Optional.ofNullable(rowsRead);
    }

    /**
     * Get the weight of the read data if present.
     *
     * @return weight of the read data if present.
     */
    public Optional<Long> getDataWeightRead() {
        return Optional.ofNullable(dataWeightRead);
    }

    /**
     * Get the number of rows written if present.
     *
     * @return number of rows written if present.
     */
    public Optional<Long> getRowsWritten() {
        return Optional.ofNullable(rowsWritten);
    }

    /**
     * Get the sync time if present.
     *
     * @return sync time if present.
     */
    public Optional<Duration> getSyncTime() {
        return Optional.ofNullable(syncTime);
    }

    /**
     * Get the async time if present.
     *
     * @return async time if present.
     */
    public Optional<Duration> getAsyncTime() {
        return Optional.ofNullable(asyncTime);
    }

    /**
     * Get the execution time if present.
     *
     * @return execution time if present.
     */
    public Optional<Duration> getExecuteTime() {
        return Optional.ofNullable(executeTime);
    }

    /**
     * Get the read time if present.
     *
     * @return read time if present.
     */
    public Optional<Duration> getReadTime() {
        return Optional.ofNullable(readTime);
    }

    /**
     * Get the write time if present.
     *
     * @return write time if present.
     */
    public Optional<Duration> getWriteTime() {
        return Optional.ofNullable(writeTime);
    }

    /**
     * Get the codegen time if present.
     *
     * @return codegen time if present.
     */
    public Optional<Duration> getCodegenTime() {
        return Optional.ofNullable(codegenTime);
    }

    /**
     * Get the wait on ready event time if present.
     *
     * @return wait on ready event time if present.
     */
    public Optional<Duration> getWaitOnReadyEventTime() {
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
     * Get the memory usage if present.
     *
     * @return memory usage if present.
     */
    public Optional<Long> getMemoryUsage() {
        return Optional.ofNullable(memoryUsage);
    }

    /**
     * Get the total grouped row count if present.
     *
     * @return total grouped row count if present.
     */
    public Optional<Long> getTotalGroupedRowCount() {
        return Optional.ofNullable(totalGroupedRowCount);
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
