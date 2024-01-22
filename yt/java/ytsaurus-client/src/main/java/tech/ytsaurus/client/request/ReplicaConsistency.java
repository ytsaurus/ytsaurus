package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EReplicaConsistency;

public enum ReplicaConsistency {
    Sync(EReplicaConsistency.RRM_SYNC, "sync"),
    None(EReplicaConsistency.RRM_NONE, "none");

    private final EReplicaConsistency protoValue;
    private final String stringValue;

    ReplicaConsistency(EReplicaConsistency protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    EReplicaConsistency getProtoValue() {
        return protoValue;
    }
}
