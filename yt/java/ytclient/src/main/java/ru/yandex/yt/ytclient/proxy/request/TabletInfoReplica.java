package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;

public class TabletInfoReplica {
    private final GUID replicaId;
    private final long lastReplicationTimestamp;

    public TabletInfoReplica(GUID replicaId, long lastReplicationTimestamp) {
        this.replicaId = replicaId;
        this.lastReplicationTimestamp = lastReplicationTimestamp;
    }

    public GUID getReplicaId() {
        return replicaId;
    }

    public long getLastReplicationTimestamp() {
        return lastReplicationTimestamp;
    }
}
