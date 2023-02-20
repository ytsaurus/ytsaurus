package tech.ytsaurus.client.operations;

import java.io.Closeable;
import java.util.Iterator;


import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.ysontree.YTreeMapNode;


/**
 * Allows writing statistics from user job script.
 *
 * @see Mapper#map(Iterator, Yield, Statistics, OperationContext)
 * @see StatisticsImpl
 */
@NonNullApi
public interface Statistics extends Closeable {

    default void start(String jobName) {
    }

    default void finish() {
    }

    default void write(YTreeMapNode metricsDict) {
    }

}
