package ru.yandex.yt.ytclient.proxy.locks;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.GetNode;
import ru.yandex.yt.ytclient.proxy.request.LockMode;
import ru.yandex.yt.ytclient.proxy.request.LockNode;
import ru.yandex.yt.ytclient.proxy.request.SetNode;

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
    protected CypressLockProviderBase(YPath rootPath,
                                      Duration pingPeriod,
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
    }

}
