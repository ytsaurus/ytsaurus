package ru.yandex.yt.ytclient.operations;

import java.io.Closeable;

import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;

public interface Statistics extends Closeable {

    default void start(String jobName) {
    }

    default void finish() {
    }

    void write(YTreeMapNode metricsDict);

}
