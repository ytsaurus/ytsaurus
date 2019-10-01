package ru.yandex.allotment;

public enum Status {
    OK(0),
    WARN(1),
    ERR(2);

    private final int index;

    Status(int index) {
        this.index = index;
    }

    public int index() {
        return index;
    }

    public Status plus(Status other) {
        if (index > other.index) {
            return this;
        } else {
            return other;
        }
    }
}
