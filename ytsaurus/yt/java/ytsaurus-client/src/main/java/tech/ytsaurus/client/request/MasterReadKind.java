package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EMasterReadKind;

public enum MasterReadKind {
    Leader("leader", EMasterReadKind.MRK_LEADER),
    Follower("follower", EMasterReadKind.MRK_FOLLOWER),
    Cache("cache", EMasterReadKind.MRK_CACHE),
    MasterCache("master_cache", EMasterReadKind.MRK_MASTER_CACHE);

    private final String wireName;
    private final EMasterReadKind protoValue;

    MasterReadKind(String wireName, EMasterReadKind protoValue) {
        this.wireName = wireName;
        this.protoValue = protoValue;
    }

    public EMasterReadKind getProtoValue() {
        return protoValue;
    }

    /**
     * Get string that is used in http interface.
     */
    public String getWireName() {
        return wireName;
    }
}
