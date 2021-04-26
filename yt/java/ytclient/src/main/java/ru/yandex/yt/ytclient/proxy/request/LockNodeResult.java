package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.common.GUID;

@SuppressWarnings("checkstyle:VisibilityModifier")
public class LockNodeResult {
    public final GUID nodeId;
    public final GUID lockId;

    public LockNodeResult(GUID n, GUID l) {
        nodeId = n;
        lockId = l;
    }
}
