package tech.ytsaurus.client.operations;

import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
final class YTableEntryTypes {
    public static final YTableEntryType<YTreeMapNode> YSON = new YsonTableEntryType(false, false);

    private YTableEntryTypes() {
    }
}
