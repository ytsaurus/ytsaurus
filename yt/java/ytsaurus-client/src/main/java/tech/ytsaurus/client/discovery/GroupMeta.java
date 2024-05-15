package tech.ytsaurus.client.discovery;

import tech.ytsaurus.TGroupMeta;

public class GroupMeta {
    private final int memberCount;

    public GroupMeta(int memberCount) {
        this.memberCount = memberCount;
    }

    public static GroupMeta fromProto(TGroupMeta protoValue) {
        return new GroupMeta(protoValue.getMemberCount());
    }

    public TGroupMeta toProto() {
        TGroupMeta.Builder builder = TGroupMeta.newBuilder();
        builder.setMemberCount(memberCount);
        return builder.build();
    }

    public int getMemberCount() {
        return memberCount;
    }
}
