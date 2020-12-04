package ru.yandex.yt.ytclient.proxy.request;

public class TabletInfoReplica {
    private final long lastReplicationTimestamp;

    public TabletInfoReplica(long lastReplicationTimestamp) {
        this.lastReplicationTimestamp = lastReplicationTimestamp;
    }

    public long getLastReplicationTimestamp() {
        return lastReplicationTimestamp;
    }
}
