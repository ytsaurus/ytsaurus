package tech.ytsaurus.client.request;

import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.rpcproxy.TRspListQueries;

/**
 * Immutable list queries result.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#listQueries(ListQueries)
 */
public class ListQueriesResult {
    private final List<Query> queries;
    private final boolean incomplete;
    private final YtTimestamp timestamp;

    public ListQueriesResult(TRspListQueries rsp) {
        this.queries = rsp.getQueriesList().stream()
                .map(Query::new)
                .collect(Collectors.toList());
        this.incomplete = rsp.getIncomplete();
        this.timestamp = YtTimestamp.valueOf(rsp.getTimestamp());
    }

    /**
     * Get queries.
     *
     * @return queries.
     */
    public List<Query> getQueries() {
        return queries;
    }

    /**
     * Whether result is incomplete.
     *
     * @return true if result is incomplete, false otherwise.
     */
    public boolean isIncomplete() {
        return incomplete;
    }

    /**
     * Get timestamp.
     *
     * @return timestamp.
     */
    public YtTimestamp getTimestamp() {
        return timestamp;
    }
}
