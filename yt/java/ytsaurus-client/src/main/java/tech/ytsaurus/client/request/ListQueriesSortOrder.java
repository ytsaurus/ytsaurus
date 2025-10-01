package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EListQueriesSortOrder;

// ListQueriesSortOrder specifies the order of the results of the ListQueries call.
public enum ListQueriesSortOrder {
    Cursor(EListQueriesSortOrder.LQSO_CURSOR, "cursor"),
    Ascending(EListQueriesSortOrder.LQSO_ASCENDING, "ascending"),
    Descending(EListQueriesSortOrder.LQSO_DESCENDING, "descending");

    private final EListQueriesSortOrder protoValue;
    private final String stringValue;

    ListQueriesSortOrder(EListQueriesSortOrder protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    EListQueriesSortOrder getProtoValue() {
        return protoValue;
    }
}
