package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import NYT.NChunkClient.NProto.DataStatistics;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TRspGetQueryResult;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable metadata about query result.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getQueryResult(GetQueryResult)
 */
public class QueryResult {
    private final GUID queryId;
    private final long resultIndex;
    private final YTsaurusError error;
    @Nullable
    private final TableSchema schema;
    private final DataStatistics.TDataStatistics dataStatistics;
    private final boolean isTruncated;
    @Nullable
    private final YTreeNode fullResult;

    public QueryResult(TRspGetQueryResult rsp) {
        this.queryId = RpcUtil.fromProto(rsp.getQueryId());
        this.resultIndex = rsp.getResultIndex();
        this.error = new YTsaurusError(rsp.getError());
        this.schema = rsp.hasSchema() ? ApiServiceUtil.deserializeTableSchema(rsp.getSchema()) : null;
        this.dataStatistics = rsp.getDataStatistics();
        this.isTruncated = rsp.getIsTruncated();
        this.fullResult = rsp.hasFullResult() ? RpcUtil.parseByteString(rsp.getFullResult()) : null;
    }

    /**
     * Get query id.
     *
     * @return query id.
     */
    public GUID getQueryId() {
        return queryId;
    }

    /**
     * Get result index.
     *
     * @return result index.
     */
    public long getResultIndex() {
        return resultIndex;
    }

    /**
     * Get result error.
     *
     * @return error.
     */
    public YTsaurusError getError() {
        return error;
    }

    /**
     * Get result table schema if present.
     *
     * @return optional table schema.
     */
    public Optional<TableSchema> getSchema() {
        return Optional.ofNullable(schema);
    }

    /**
     * Get result data statistics.
     *
     * @return data statistics.
     */
    public DataStatistics.TDataStatistics getDataStatistics() {
        return dataStatistics;
    }

    /**
     * Whether result is truncated.
     *
     * @return true if result is truncated, false otherwise.
     */
    public boolean isTruncated() {
        return isTruncated;
    }

    /**
     * Table with full result. For yql it's yson containing 'cluster' and 'table_path'
     *
     * @return optional YSON map.
     */
    public Optional<YTreeNode> fullResult() {
        return Optional.ofNullable(fullResult);
    }
}
