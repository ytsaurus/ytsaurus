package ru.yandex.yt.ytclient.proxy;

import java.util.concurrent.CompletableFuture;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.proxy.request.ConcatenateNodes;
import ru.yandex.yt.ytclient.proxy.request.CopyNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LinkNode;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.proxy.request.MoveNode;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;

public interface TransactionalClient {
    CompletableFuture<GUID> createNode(CreateNode req);
    CompletableFuture<Void> removeNode(RemoveNode req);
    CompletableFuture<Void> setNode(SetNode req);
    CompletableFuture<YTreeNode> getNode(GetNode req);
    CompletableFuture<YTreeNode> listNode(ListNode req);
    CompletableFuture<GUID> copyNode(CopyNode req);
    CompletableFuture<GUID> linkNode(LinkNode req);
    CompletableFuture<GUID> moveNode(MoveNode req);
    CompletableFuture<Boolean> existsNode(ExistsNode req);
    CompletableFuture<Void> concatenateNodes(ConcatenateNodes req);
}
