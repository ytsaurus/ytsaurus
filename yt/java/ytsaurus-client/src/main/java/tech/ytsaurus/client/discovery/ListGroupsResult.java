package tech.ytsaurus.client.discovery;

import java.util.List;

import tech.ytsaurus.TRspListGroups;

public class ListGroupsResult {
    private final List<String> groupIds;
    private final boolean incomplete;

    public ListGroupsResult(TRspListGroups rsp) {
        this.groupIds = List.copyOf(rsp.getGroupIdsList());
        this.incomplete = rsp.getIncomplete();
    }

    public List<String> getGroupIds() {
        return groupIds;
    }

    public boolean isIncomplete() {
        return incomplete;
    }
}
