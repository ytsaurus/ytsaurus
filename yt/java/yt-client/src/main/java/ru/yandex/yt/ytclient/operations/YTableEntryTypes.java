package ru.yandex.yt.ytclient.operations;

import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;

public final class YTableEntryTypes {
    public static final YTableEntryType<YTreeMapNode> YSON = new YsonTableEntryType(false, false);

    private YTableEntryTypes() {
    }
}
