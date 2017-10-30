package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.ytclient.misc.YtGuid;

public class LockNodeResult {
    public final YtGuid nodeId;
    public final YtGuid lockId;

    public LockNodeResult(YtGuid n, YtGuid l) {
        nodeId = n;
        lockId = l;
    }
}
