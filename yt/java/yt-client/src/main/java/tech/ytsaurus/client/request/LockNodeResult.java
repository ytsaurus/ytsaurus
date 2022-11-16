package tech.ytsaurus.client.request;

import tech.ytsaurus.core.GUID;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class LockNodeResult {
    public final GUID nodeId;
    public final GUID lockId;

    public LockNodeResult(GUID n, GUID l) {
        nodeId = n;
        lockId = l;
    }
}
