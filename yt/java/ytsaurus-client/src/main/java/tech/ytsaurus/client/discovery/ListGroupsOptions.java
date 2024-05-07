package tech.ytsaurus.client.discovery;

import tech.ytsaurus.TListGroupsOptions;

public class ListGroupsOptions {
    private final int limit;

    public ListGroupsOptions(int limit) {
        this.limit = limit;
    }

    public ListGroupsOptions() {
        this(100);
    }

    public static ListGroupsOptions fromProto(TListGroupsOptions protoValue) {
        return new ListGroupsOptions(protoValue.getLimit());
    }

    public TListGroupsOptions toProto() {
        TListGroupsOptions.Builder builder = TListGroupsOptions.newBuilder();
        builder.setLimit(limit);
        return builder.build();
    }
}
