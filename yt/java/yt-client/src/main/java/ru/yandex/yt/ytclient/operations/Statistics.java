package ru.yandex.yt.ytclient.operations;

import java.io.Closeable;


import tech.ytsaurus.ysontree.YTreeMapNode;
public interface Statistics extends Closeable {

    default void start(String jobName) {
    }

    default void finish() {
    }

    void write(YTreeMapNode metricsDict);

}
