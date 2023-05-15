package tech.ytsaurus.client.operations;

import com.google.protobuf.Message;
import tech.ytsaurus.core.utils.ProtoUtils;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeMapNode;


@NonNullApi
@NonNullFields
final class YTableEntryTypes {
    private YTableEntryTypes() {
    }

    public static YTableEntryType<YTreeMapNode> yson(boolean trackIndices) {
        return new YsonTableEntryType(trackIndices, trackIndices);
    }

    public static <T> YTableEntryType<T> entity(
            Class<T> clazz,
            boolean trackIndices,
            boolean isInputType
    ) {
        return new EntityTableEntryType<>(clazz, trackIndices, isInputType);
    }

    public static <T extends Message> YTableEntryType<T> proto(
            Class<T> clazz,
            boolean trackIndices,
            boolean isInputType
    ) {
        return new ProtobufTableEntryType<>(ProtoUtils.newBuilder(clazz), trackIndices, isInputType);
    }
}
