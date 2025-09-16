package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.core.cypress.YPath;

public class MultiTablePartition {
    private final List<YPath> tableRanges;
    private final TablePartitionCookie cookie;
    private final AggregateStatistics statistics;

    public MultiTablePartition(List<YPath> tableRanges, TablePartitionCookie cookie, AggregateStatistics statistics) {
        this.tableRanges = new ArrayList<>(tableRanges);
        this.cookie = cookie;
        this.statistics = statistics;
    }

    public List<YPath> getTableRanges() {
        return tableRanges;
    }

    public TablePartitionCookie getCookie() {
        return cookie;
    }

    public AggregateStatistics getStatistics() {
        return statistics;
    }

    public static class AggregateStatistics {
        private final long chunkCount;
        private final long dataWeight;
        private final long rowCount;

        public AggregateStatistics(long chunkCount, long dataWeight, long rowCount) {
            this.chunkCount = chunkCount;
            this.dataWeight = dataWeight;
            this.rowCount = rowCount;
        }

        public long getChunkCount() {
            return chunkCount;
        }

        public long getDataWeight() {
            return dataWeight;
        }

        public long getRowCount() {
            return rowCount;
        }
    }
}
