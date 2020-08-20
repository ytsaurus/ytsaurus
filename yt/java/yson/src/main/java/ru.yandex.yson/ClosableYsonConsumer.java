package ru.yandex.yson;

public interface ClosableYsonConsumer extends YsonConsumer, AutoCloseable {
    void close();
}
