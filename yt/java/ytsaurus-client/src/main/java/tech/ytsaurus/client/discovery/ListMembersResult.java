package tech.ytsaurus.client.discovery;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import tech.ytsaurus.TRspListMembers;

public class ListMembersResult {
    private final List<MemberInfo> members;

    public ListMembersResult(TRspListMembers rsp) {
        this.members = rsp.getMembersList().stream().map(MemberInfo::fromProto).collect(Collectors.toList());
    }

    public List<MemberInfo> getMembers() {
        return Collections.unmodifiableList(members);
    }
}
