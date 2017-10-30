package ru.yandex.yt.ytclient.proxy.request;

public enum LockMode {
    None (0),
    Snapshot (1),
    Shared (2),
    Exclusive (3);

    private final int value;

    LockMode(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
