package tech.ytsaurus.yson;

public interface ClosableYsonConsumer extends YsonConsumer, AutoCloseable {
    void close();
}
