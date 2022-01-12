package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.locks.CypressLockProviderBase;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcErrorCode;

@NonNullApi
@NonNullFields
public final class CypressLockProvider extends CypressLockProviderBase<CypressLockProvider.LockImpl> {

    private static final Logger log = LoggerFactory.getLogger(CypressLockProvider.class);
    private final YtClient ytClient;

    public CypressLockProvider(YtClient ytClient, YPath rootPath, Duration pingPeriod,
                               Duration failedPingRetryPeriod, Duration checkLockStatePeriod) {
        super(rootPath, pingPeriod, failedPingRetryPeriod, checkLockStatePeriod);
        this.ytClient = ytClient;
    }

    public CypressLockProvider(YtClient ytClient, YPath rootPath, Duration pingPeriod,
                               Duration failedPingRetryPeriod) {
        this(ytClient, rootPath, pingPeriod, failedPingRetryPeriod, failedPingRetryPeriod);
    }

    @Override
    protected LockImpl newLockImpl(YPath lockPath, @Nullable Consumer<Exception> onPingFailed,
                                   LockMode lockMode, Map<String, YTreeNode> attributes) {
        return new LockImpl(lockPath, onPingFailed, lockMode, attributes);
    }

    @Override
    protected void createNode(CreateNode node) {
        ytClient.createNode(node).join();
    }

    @Override
    protected YTreeNode getNode(GetNode node) {
        return ytClient.getNode(node).join();
    }

    @Override
    protected GUID lockNode(LockNode node) {
        return ytClient.lockNode(node).join().lockId;
    }

    @Override
    protected void setNode(SetNode node) {
        ytClient.setNode(node).join();
    }

    public final class LockImpl extends CypressLockProviderBase<LockImpl>.LockImpl {

        @Nullable private ApiServiceTransaction transaction;

        private LockImpl(YPath lockPath, @Nullable Consumer<Exception> onPingFailed,
                         LockMode lockMode, Map<String, YTreeNode> attributes) {
            super(lockPath, onPingFailed, lockMode, attributes);
        }

        @Override
        public String toString() {
            return "YtClientLock{" + lockPath + '}';
        }

        @Override
        public boolean isTaken() {
            synchronized (monitor) {
                if (transaction == null) {
                    return false;
                }
                if (transaction.isActive()) {
                    return isTaken;
                } else {
                    isTaken = false;
                    return false;
                }
            }
        }

        @Override
        protected GUID startTransaction() {
            StartTransaction startTransaction = StartTransaction.master()
                    .setPingPeriod(pingPeriod)
                    .setFailedPingRetryPeriod(failedPingRetryPeriod)
                    .setPingAncestors(false)
                    .setTimeout(pingPeriod.multipliedBy(2))
                    .setOnPingFailed(onPingFailed)
                    .setAttributes(attributes);
            ApiServiceTransaction newTransaction = ytClient.startTransaction(startTransaction).join();
            GUID transactionId = newTransaction.getId();
            synchronized (monitor) {
                transaction = newTransaction;
                isTaken = false;
            }
            log.debug("started transaction {} with pinging for lock {}", transactionId, lockPath);
            return transactionId;
        }

        @Override
        protected void abortTransaction() {
            synchronized (monitor) {
                if (transaction == null) {
                    throw new IllegalStateException("Trying to abort transaction without starting it");
                }
                transaction.abort().join();
            }
        }

        @Override
        protected void unsetTransaction() {
            synchronized (monitor) {
                transaction = null;
                isTaken = false;
            }
        }

        @Override
        protected boolean isTransactionActive() {
            synchronized (monitor) {
                if (transaction == null) {
                    return false;
                }
                return transaction.isActive();
            }
        }

        @Override
        public synchronized void unlock() {
            if (transaction == null) {
                throw new IllegalStateException("Lock is not locked");
            }
            try {
                transaction.commit().join();
            } finally {
                unsetTransaction();
            }
        }

        @Override
        public synchronized GUID getTransactionId() {
            ApiServiceTransaction currentTransaction = transaction;
            if (currentTransaction == null) {
                throw new IllegalStateException("Get Transaction id without acquired lock");
            }
            return currentTransaction.getId();
        }

        @Override
        protected boolean isConcurrentLockConflict(Throwable error) {
            if (error instanceof RpcError) {
                RpcError rpcError = (RpcError) error;
                return rpcError.getError().getCode() == RpcErrorCode.ConcurrentTransactionLockConflict.code;
            }
            return false;
        }
    }
}
