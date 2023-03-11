package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EDurability;

/**
 * Durability of transactions.
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables">
 * dynamic table documentation
 * </a>
 */
public enum Durability {
    Sync(EDurability.D_SYNC, "sync"),
    Async(EDurability.D_ASYNC, "async");

    private final EDurability protoValue;
    private final String stringValue;

    Durability(EDurability protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public EDurability getProtoValue() {
        return protoValue;
    }
}
