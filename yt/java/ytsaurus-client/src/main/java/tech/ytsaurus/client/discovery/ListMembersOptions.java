package tech.ytsaurus.client.discovery;

import java.util.List;

import tech.ytsaurus.TListMembersOptions;

public class ListMembersOptions {
    private final int limit;

    private final List<String> attributeKeys;

    public ListMembersOptions(int limit, List<String> attributeKeys) {
        this.limit = limit;
        this.attributeKeys = attributeKeys;
    }

    public ListMembersOptions(int limit) {
        this(limit, List.of());
    }

    public ListMembersOptions() {
        this(100);
    }

    public static ListMembersOptions fromProto(TListMembersOptions protoValue) {
        return new ListMembersOptions(protoValue.getLimit(), List.copyOf(protoValue.getAttributeKeysList()));
    }

    public TListMembersOptions toProto() {
        TListMembersOptions.Builder builder = TListMembersOptions.newBuilder();
        builder.setLimit(limit);
        builder.addAllAttributeKeys(attributeKeys);
        return builder.build();
    }
}
