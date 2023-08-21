package tech.ytsaurus.client.request;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpcproxy.ETableReplicaMode;

public class TabletInfoReplica {
    private final GUID replicaId;
    private final long lastReplicationTimestamp;
    private final TableReplicaMode mode;

    public TabletInfoReplica(GUID replicaId, long lastReplicationTimestamp, ETableReplicaMode mode) {
        this.replicaId = replicaId;
        this.lastReplicationTimestamp = lastReplicationTimestamp;
        this.mode = TableReplicaMode.fromProtoValue(mode);
    }

    public GUID getReplicaId() {
        return replicaId;
    }

    public long getLastReplicationTimestamp() {
        return lastReplicationTimestamp;
    }

    public TableReplicaMode getMode() {
        return mode;
    }
}
