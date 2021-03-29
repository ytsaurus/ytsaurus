package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public enum LockMode {
    Snapshot("snapshot", 1),
    Shared("shared", 2),
    Exclusive("exclusive", 3);

    private final int protoValue;
    private final String wireName;

    LockMode(String wireName, int protoValue) {
        this.wireName = wireName;
        this.protoValue = protoValue;
    }

    public static LockMode of(ru.yandex.inside.yt.kosher.cypress.LockMode lockMode) {
        switch (lockMode) {
            case SNAPSHOT:
                return Snapshot;
            case SHARED:
                return Shared;
            case EXCLUSIVE:
                return Exclusive;
            default:
                throw new IllegalArgumentException("Unknown lock mode: " + lockMode);
        }
    }

    public int getProtoValue() {
        return protoValue;
    }

    public String getWireName() {
        return wireName;
    }

}
