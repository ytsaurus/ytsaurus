package tech.ytsaurus.client.sync;

import java.util.concurrent.CompletionException;

import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.OperationStatus;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.ysontree.YTreeNode;

public class SyncOperationImpl implements SyncOperation {
    private final Operation operation;

    public SyncOperationImpl(Operation operation) {
        this.operation = operation;
    }

    @Override
    public GUID getId() {
        return operation.getId();
    }

    @Override
    public OperationStatus getStatus() {
        return operation.getStatus().join();
    }

    @Override
    public YTreeNode getResult() {
        return operation.getResult().join();
    }

    @Override
    public void watch() {
        operation.watch().join();
    }

    @Override
    public void watchAndThrowIfNotSuccess() {
        try {
            operation.watchAndThrowIfNotSuccess().join();
        } catch (CompletionException e) {
            var cause = e.getCause();
            if (cause instanceof YTsaurusError) {
                throw (YTsaurusError) cause;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort() {
        operation.abort().join();
    }
}
