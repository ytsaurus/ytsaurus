package tech.ytsaurus.client.sync;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;

public class SyncApiServiceTransaction extends SyncTransactionalClientImpl implements AutoCloseable {
    private final ApiServiceTransaction transaction;

    private SyncApiServiceTransaction(ApiServiceTransaction transaction) {
        super(transaction);
        this.transaction = transaction;
    }

    public static SyncApiServiceTransaction wrap(ApiServiceTransaction transaction) {
        return new SyncApiServiceTransaction(transaction);
    }

    public SyncApiServiceClientImpl getClient() {
        return SyncApiServiceClientImpl.wrap(transaction.getClient());
    }

    public GUID getId() {
        return transaction.getId();
    }

    public TransactionalOptions getTransactionalOptions() {
        return transaction.getTransactionalOptions();
    }

    public YtTimestamp getStartTimestamp() {
        return transaction.getStartTimestamp();
    }

    public boolean isPing() {
        return transaction.isPing();
    }

    public boolean isSticky() {
        return transaction.isSticky();
    }

    public void stopPing() {
        transaction.stopPing();
    }

    public void ping() {
        transaction.ping().join();
    }

    public void commit() {
        transaction.commit().join();
    }

    public void abort() {
        transaction.abort().join();
    }

    @Override
    public void close() {
        transaction.close();
    }
}
