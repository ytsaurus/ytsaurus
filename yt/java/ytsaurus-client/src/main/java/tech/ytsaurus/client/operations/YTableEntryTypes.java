package tech.ytsaurus.client.operations;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeMapNode;


@NonNullApi
@NonNullFields
final class YTableEntryTypes {
    public static final YTableEntryType<YTreeMapNode> YSON = new YsonTableEntryType(false, false);

    private YTableEntryTypes() {
    }
}
