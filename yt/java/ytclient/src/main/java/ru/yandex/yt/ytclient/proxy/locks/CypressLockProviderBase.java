package ru.yandex.yt.ytclient.proxy.locks;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;

import static java.lang.Math.min;

@NonNullApi
@NonNullFields
public abstract class CypressLockProviderBase<T extends CypressLockProviderBase<T>.LockImpl> {

    protected final YPath rootPath;
    protected final Duration pingPeriod;
    protected final Duration failedPingRetryPeriod;
    protected final Duration checkLockStatePeriod;

    /**
     * @param rootPath              root path, where locks will be created
     * @param pingPeriod            interval between a successful ping and a new ping attempt
     * @param failedPingRetryPeriod interval between an unsuccessful ping and a new ping attempt
     * @param checkLockStatePeriod  interval between checking the lock state during an attempt to acquire lock
     */
    protected CypressLockProviderBase(YPath rootPath, Duration pingPeriod,
                                      Duration failedPingRetryPeriod,
                                      Duration checkLockStatePeriod) {
        this.rootPath = rootPath;
        this.pingPeriod = pingPeriod;
        this.failedPingRetryPeriod = failedPingRetryPeriod;
        this.checkLockStatePeriod = checkLockStatePeriod;
    }

    public T createLock(String name) {
        return createLock(name, null, LockMode.Exclusive);
    }

    public T createLock(String name, @Nullable Consumer<Exception> onPingFailed) {
        return createLock(name, onPingFailed, LockMode.Exclusive);
    }

    public T createLock(String name, ru.yandex.inside.yt.kosher.cypress.LockMode lockMode) {
        return createLock(name, null, LockMode.of(lockMode));
    }

    public T createLock(String name, LockMode lockMode) {
        return createLock(name, null, lockMode);
    }

    public T createLock(String name, @Nullable Consumer<Exception> onPingFailed,
                        ru.yandex.inside.yt.kosher.cypress.LockMode lockMode) {
        return createLock(name, onPingFailed, LockMode.of(lockMode));
    }

    public T createLock(String name, @Nullable Consumer<Exception> onPingFailed, LockMode lockMode) {
        return createLock(name, onPingFailed, lockMode, Collections.emptyMap());
    }

    public T createLock(String name, @Nullable Consumer<Exception> onPingFailed, LockMode lockMode,
                        Map<String, YTreeNode> attributes) {
        YPath lockPath = rootPath.child(name);

        createNode(new CreateNode(lockPath, CypressNodeType.MAP)
                .setRecursive(true)
                .setIgnoreExisting(true));
        return newLockImpl(lockPath, onPingFailed, lockMode, attributes);
    }

    public T attachLock(String name) {
        return attachLock(name, null);
    }

    public T attachLock(String name, @Nullable Consumer<Exception> onPingFailed) {
        YPath lockPath = rootPath.child(name);
        return newLockImpl(lockPath, onPingFailed, LockMode.Exclusive, Collections.emptyMap());
    }

    protected abstract T newLockImpl(YPath lockPath, @Nullable Consumer<Exception> onPingFailed,
                                     LockMode lockMode, Map<String, YTreeNode> attributes);

    protected abstract void createNode(CreateNode node);

    protected abstract YTreeNode getNode(GetNode node);

    protected abstract GUID lockNode(LockNode node);

    protected abstract void setNode(SetNode node);

    public abstract class LockImpl implements Lock {

        protected final Object monitor = new Object();
        protected final YPath lockPath;
        @Nullable
        protected final Consumer<Exception> onPingFailed;
        protected final LockMode lockMode;
        protected final Map<String, YTreeNode> attributes = new HashMap<>();
        protected boolean isTaken;

        protected LockImpl(YPath lockPath, @Nullable Consumer<Exception> onPingFailed,
                           LockMode lockMode, Map<String, YTreeNode> attributes) {
            this.lockPath = lockPath;
            this.onPingFailed = onPingFailed;
            this.lockMode = lockMode;
            this.attributes.putAll(attributes);
        }

        public abstract boolean isTaken();

        protected void setTaken() {
            synchronized (monitor) {
                isTaken = true;
            }
        }

        protected abstract GUID startTransaction();

        protected abstract void abortTransaction();

        protected abstract void unsetTransaction();

        protected abstract boolean isTransactionActive();

        protected abstract boolean isConcurrentLockConflict(Throwable error);

        @Override
        public synchronized boolean tryLock() {
            if (isTaken()) {
                return false;
            }

            GUID transactionId = startTransaction();

            try {
                LockNode lockNode = new LockNode(lockPath, lockMode)
                        .setTransactionalOptionsOfTransactionId(transactionId);
                lockNode(lockNode);
            } catch (Exception e) {
                Throwable error = e instanceof CompletionException ? e.getCause() : e;
                try {
                    abortTransaction();
                } catch (Exception e1) {
                    e.addSuppressed(e1);
                } finally {
                    unsetTransaction();
                }
                if (isConcurrentLockConflict(error)) {
                    return false;
                }
                throw e;
            }
            setTaken();

            return true;
        }

        private synchronized boolean tryLockInternal(long time, TimeUnit unit, boolean allowInterrupts)
                throws InterruptedException {
            if (isTaken()) {
                throw new ReentrantLockException();
            }
            if (unit.toMillis(time) <= 1) {
                return tryLock();
            }

            long deadline = System.currentTimeMillis() + unit.toMillis(time);
            GUID transactionId = startTransaction();

            GUID lockId;
            try {
                LockNode lockNode = new LockNode(lockPath, lockMode)
                        .setTransactionalOptionsOfTransactionId(transactionId)
                        .setWaitable(true);
                lockId = lockNode(lockNode);
            } catch (Exception e) {
                try {
                    abortTransaction();
                } catch (Exception e1) {
                    e.addSuppressed(e1);
                } finally {
                    unsetTransaction();
                }
                throw e;
            }

            GetNode stateNode = new GetNode(YPath.simple("#" + lockId + "/@state"));
            try {
                String state = getNode(stateNode).stringValue();
                while (!"acquired".equalsIgnoreCase(state)) {
                    if (!isTransactionActive()) {
                        break;
                    }
                    long waitDuration = min(checkLockStatePeriod.toMillis(), deadline - System.currentTimeMillis());
                    if (waitDuration < 1) {
                        break;
                    }
                    try {
                        Thread.sleep(waitDuration);
                    } catch (InterruptedException e) {
                        if (allowInterrupts) {
                            throw e;
                        }
                    }
                    state = getNode(stateNode).stringValue();
                }

                if (!"acquired".equalsIgnoreCase(state)) {
                    lockId = null;
                }
            } catch (Exception e) {
                try {
                    abortTransaction();
                } catch (Exception e1) {
                    e.addSuppressed(e1);
                } finally {
                    unsetTransaction();
                }
                throw e;
            }

            if (lockId == null) {
                try {
                    abortTransaction();
                } finally {
                    unsetTransaction();
                }
                return false;
            } else {
                setTaken();
                return true;
            }
        }

        @Override
        public synchronized void lock() {
            while (true) {
                try {
                    if (tryLockInternal(1000, TimeUnit.DAYS, false)) {
                        return;
                    }
                } catch (InterruptedException ignored) {
                    // suppress
                }
            }
        }

        @Override
        public synchronized void lockInterruptibly() throws InterruptedException {
            while (true) {
                if (tryLockInternal(1000, TimeUnit.DAYS, true)) {
                    return;
                }
            }
        }

        @Override
        public synchronized boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return tryLockInternal(time, unit, true);
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        /**
         * Create uint64 node in Cypress if it doesn't exist yet.
         *
         * @param path Path to node to be created.
         */
        public synchronized void initEpoch(YPath path) {
            createNode(
                    new CreateNode(path, CypressNodeType.UINT64).setRecursive(true).setIgnoreExisting(true)
            );
        }

        public synchronized long tryIncrementEpoch(YPath path) {
            GUID transactionId = getTransactionId();
            long currentEpoch = getNode(new GetNode(path)).longValue();
            SetNode setNode = new SetNode(path, YTree.integerNode(currentEpoch + 1))
                    .setRecursive(false)
                    .setForce(false)
                    .setTransactionalOptionsOfTransactionId(transactionId);
            setNode(setNode);
            return currentEpoch;
        }

        public abstract GUID getTransactionId();
    }
}
