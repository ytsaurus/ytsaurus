package tech.ytsaurus.client.sync;

import tech.ytsaurus.client.operations.OperationStatus;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

public interface SyncOperation {
    GUID getId();

    OperationStatus getStatus();

    YTreeNode getResult();

    void watch();

    void watchAndThrowIfNotSuccess();

    void abort();
}
