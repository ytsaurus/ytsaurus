package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.ETableReplicaMode;

public enum TableReplicaMode {
    Sync(ETableReplicaMode.TRM_SYNC, "sync"),
    Async(ETableReplicaMode.TRM_ASYNC, "async");

    private final ETableReplicaMode protoValue;
    private final String stringValue;

    TableReplicaMode(ETableReplicaMode protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public ETableReplicaMode getProtoValue() {
        return protoValue;
    }
}
