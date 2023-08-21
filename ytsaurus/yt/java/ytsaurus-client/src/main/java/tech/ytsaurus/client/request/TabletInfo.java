package tech.ytsaurus.client.request;

import java.util.List;

public class TabletInfo {
    private final long totalRowCount;
    private final long trimmedRowCount;
    private final long lastWriteTimestamp;
    private final List<TabletInfoReplica> tabletInfoReplicas;

    public TabletInfo(long totalRowCount, long trimmedRowCount,
                      long lastWriteTimestamp,
                      List<TabletInfoReplica> tabletInfoReplicas) {
        this.totalRowCount = totalRowCount;
        this.trimmedRowCount = trimmedRowCount;
        this.lastWriteTimestamp = lastWriteTimestamp;
        this.tabletInfoReplicas = tabletInfoReplicas;
    }

    public long getTotalRowCount() {
        return totalRowCount;
    }

    public long getTrimmedRowCount() {
        return trimmedRowCount;
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp;
    }

    public List<TabletInfoReplica> getTabletInfoReplicas() {
        return tabletInfoReplicas;
    }
}
