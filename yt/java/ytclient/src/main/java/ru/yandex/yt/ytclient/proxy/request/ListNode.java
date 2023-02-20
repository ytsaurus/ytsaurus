package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;

@NonNullApi
public class ListNode extends tech.ytsaurus.client.request.ListNode.BuilderBase<ListNode>  {
    public ListNode(String path) {
        this(YPath.simple(path));
    }

    public ListNode() {
    }

    public ListNode(YPath path) {
        setPath(path);
    }

    public ListNode(ListNode listNode) {
        super(listNode);
    }

    public ListNode(tech.ytsaurus.client.request.ListNode listNode) {
        super(listNode.toBuilder());
    }

    @Override
    protected ListNode self() {
        return this;
    }
}
