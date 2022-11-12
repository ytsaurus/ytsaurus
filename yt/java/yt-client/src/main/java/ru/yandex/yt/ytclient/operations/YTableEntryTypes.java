package ru.yandex.yt.ytclient.operations;


import tech.ytsaurus.ysontree.YTreeMapNode;
public final class YTableEntryTypes {
    public static final YTableEntryType<YTreeMapNode> YSON = new YsonTableEntryType(false, false);

    private YTableEntryTypes() {
    }
}
