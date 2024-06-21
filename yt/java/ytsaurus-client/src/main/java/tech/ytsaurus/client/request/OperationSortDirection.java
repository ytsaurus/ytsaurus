package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EOperationSortDirection;

public enum OperationSortDirection {
    None(EOperationSortDirection.OSD_NONE, "none"),
    Past(EOperationSortDirection.OSD_PAST, "past"),
    Future(EOperationSortDirection.OSD_FUTURE, "future");

    private final EOperationSortDirection protoValue;
    private final String stringValue;

    OperationSortDirection(EOperationSortDirection protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    EOperationSortDirection getProtoValue() {
        return protoValue;
    }
}
