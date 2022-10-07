package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.EDurability;

/**
 * Durability of transactions.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables#sohrannost">
 *     dynamic table documentation
 *     </a>
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
