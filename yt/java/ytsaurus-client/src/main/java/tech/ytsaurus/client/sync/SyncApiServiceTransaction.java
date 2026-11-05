package tech.ytsaurus.client.sync;

import java.util.function.Consumer;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.CommitTransaction;
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

    /**
     * Transaction id in the request is managed by this class and cannot be customized.
     */
    public void commit(Consumer<CommitTransaction.Builder> commitRequestCustomizer) {
        transaction.commit(commitRequestCustomizer).join();
    }

    public void abort() {
        transaction.abort().join();
    }

    @Override
    public void close() {
        transaction.close();
    }
}
