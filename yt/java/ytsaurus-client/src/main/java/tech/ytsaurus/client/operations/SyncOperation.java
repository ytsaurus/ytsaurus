package tech.ytsaurus.client.operations;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

public class SyncOperation {
    private final Operation operation;

    public SyncOperation(Operation operation) {
        this.operation = operation;
    }

    public GUID getId() {
        return operation.getId();
    }

    public OperationStatus getStatus() {
        return operation.getStatus().join();
    }

    public YTreeNode getResult() {
        return operation.getResult().join();
    }

    public void watch() {
        operation.watch().join();
    }

    public void abort() {
        operation.abort().join();
    }
}
