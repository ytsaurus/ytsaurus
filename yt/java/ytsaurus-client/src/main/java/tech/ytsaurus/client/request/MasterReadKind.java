package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EMasterReadKind;

public enum MasterReadKind {
    // These options cover the majority of cases.
    Leader("leader", EMasterReadKind.MRK_LEADER),
    Follower("follower", EMasterReadKind.MRK_FOLLOWER),
    Cache("cache", EMasterReadKind.MRK_CACHE),

    // These are advanced options. Typically you don't need these.
    MasterSideCache("master_side_cache", EMasterReadKind.MRK_MASTER_SIDE_CACHE);

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
