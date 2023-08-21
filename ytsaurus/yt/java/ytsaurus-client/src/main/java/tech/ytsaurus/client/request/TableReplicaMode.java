package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.ETableReplicaMode;

public enum TableReplicaMode {
    Sync(ETableReplicaMode.TRM_SYNC, "sync"),
    Async(ETableReplicaMode.TRM_ASYNC, "async");

    private final ETableReplicaMode protoValue;
    private final String stringValue;

    TableReplicaMode(ETableReplicaMode protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    public static TableReplicaMode fromProtoValue(ETableReplicaMode protoValue) {
        switch (protoValue) {
            case TRM_SYNC:
            case TRM_SYNC_TO_SYNC:
                return Sync;
            case TRM_ASYNC:
            case TRM_ASYNC_TO_SYNC:
                return Async;
            default:
                throw new IllegalArgumentException("Illegal replication mode value " + protoValue);
        }
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public ETableReplicaMode getProtoValue() {
        return protoValue;
    }
}
