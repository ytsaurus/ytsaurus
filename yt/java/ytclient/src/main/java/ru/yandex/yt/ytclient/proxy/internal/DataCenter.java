package ru.yandex.yt.ytclient.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.metrics.BalancingDestinationMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;

/**
 * @author aozeritsky
 */
public final class DataCenter {
    private static final Logger logger = LoggerFactory.getLogger(DataCenter.class);

    private static final int DEFAULT_NUMBER_OF_PROXIES_TO_PING = 3;
    private static final int DEFAULT_MAX_NUMBER_OF_PROXIES_TO_PING = 5;

    private final DataCenterMetricsHolder metricsHolder;
    private final BalancingDestinationMetricsHolder destMetricsHolder;
    private final RpcOptions options;

    private final String dc;
    private final double weight;

    private int numberOfProxiesToPing = DEFAULT_NUMBER_OF_PROXIES_TO_PING;
    private int maxNumberOfProxiesToPing = DEFAULT_MAX_NUMBER_OF_PROXIES_TO_PING;
    private Set<BalancingDestination> activeProxies;
    private Set<BalancingDestination> checkingProxies;
    private LinkedList<BalancingDestination> inactiveProxies;
    private final ReentrantLock lock = new ReentrantLock();

    public DataCenter(String dc, BalancingDestination[] backends) {
        this(dc, backends, -1.0);
    }

    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            DataCenterMetricsHolder metricsHolder,
            BalancingDestinationMetricsHolder destMetricsHolder) {
        this(dc, backends, -1.0, metricsHolder, destMetricsHolder, new RpcOptions());
    }

    public DataCenter(String dc, BalancingDestination[] backends, double weight) {
        this(dc, backends, weight, new RpcOptions());
    }

    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            double weight,
            RpcOptions options) {
        this(dc, backends, weight, options.getDataCenterMetricsHolder(), options.getDestinationMetricsHolder(), options);
    }

    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            double weight,
            DataCenterMetricsHolder metricsHolder,
            BalancingDestinationMetricsHolder destMetricsHolder,
            RpcOptions options) {
        this.dc = dc;
        this.inactiveProxies = new LinkedList<>(Arrays.asList(backends));
        this.checkingProxies = new HashSet<>();
        this.activeProxies = new HashSet<>();
        for (int i = 0; i < numberOfProxiesToPing && i < backends.length; i++) {
            final BalancingDestination client = inactiveProxies.removeFirst();
            logger.info("proxy `{}` registered as active", client);
            activeProxies.add(client);
        }

        this.weight = weight;
        this.metricsHolder = metricsHolder;
        this.destMetricsHolder = destMetricsHolder;
        this.options = options;
    }

    public double weight() {
        if (weight > 0) {
            return weight;
        } else {
            return metricsHolder.getDc99thPercentile(dc);
        }
    }

    public String getName() {
        return dc;
    }

    public void addProxies(Set<RpcClient> proxies) {
        lock.lock();
        try {
            for (RpcClient proxy : proxies) {
                BalancingDestination destination = new BalancingDestination(dc, proxy, destMetricsHolder, options);

                if (activeProxies.size() < numberOfProxiesToPing) {
                    activeProxies.add(destination);
                    checkingProxies.add(destination);
                    logger.info("added active proxy `{}` ({} out of {})", proxy,
                            activeProxies.size(), numberOfProxiesToPing);
                } else if (checkingProxies.size() < maxNumberOfProxiesToPing) {
                    checkingProxies.add(destination);
                    logger.info("added checking proxy `{}` ({} out of {})", proxy,
                            activeProxies.size(), maxNumberOfProxiesToPing);
                } else {
                    inactiveProxies.addFirst(destination);
                    logger.info("added inactive proxy `{}`", proxy);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void removeProxies(Set<RpcClient> proxies) {
        lock.lock();
        try {
            activeProxies.removeIf(destination -> {
                boolean remove = proxies.contains(destination.getClient());
                if (remove) {
                    logger.info("proxy `{}` was removed", destination);
                }
                return remove;
            });
            checkingProxies.removeIf(destination -> proxies.contains(destination.getClient()));
            inactiveProxies.removeIf(destination -> proxies.contains(destination.getClient()));
        } finally {
            lock.unlock();
        }
    }

    public boolean isAlive() {
        lock.lock();
        try {
            return activeProxies.size() > 0;
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            activeProxies.forEach(BalancingDestination::close);
            checkingProxies.forEach(BalancingDestination::close);
            inactiveProxies.forEach(BalancingDestination::close);
        } finally {
            lock.unlock();
        }
    }

    public List<RpcClient> getAliveDestinations() {
        return getAliveDestinations(BalancingDestination::getClient);
    }

    public <T> List<T> getAliveDestinations(Function<BalancingDestination, T> func) {
        lock.lock();
        try {
            return activeProxies.stream().map(func).collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    public List<RpcClient> selectDestinations(final int maxSelect) {
        return selectDestinations(maxSelect, BalancingDestination::getClient);
    }

    public <T> List<T> selectDestinations(final int maxSelect, Function<BalancingDestination, T> func) {
        final ArrayList<T> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

        lock.lock();
        try {
            for (Iterator<BalancingDestination> i = activeProxies.iterator(); i.hasNext() && result.size() < maxSelect; ) {
                BalancingDestination destination = i.next();
                result.add(func.apply(destination));
            }
        } finally {
            lock.unlock();
        }

        return result;
    }

    public int getNumberOfProxiesToPing() {
        return numberOfProxiesToPing;
    }

    public void setNumberOfProxiesToPing(int numberOfProxiesToPing) {
        this.numberOfProxiesToPing = numberOfProxiesToPing;
    }

    public int getMaxNumberOfProxiesToPing() {
        return maxNumberOfProxiesToPing;
    }

    public void setMaxNumberOfProxiesToPing(int maxNumberOfProxiesToPing) {
        this.maxNumberOfProxiesToPing = maxNumberOfProxiesToPing;
    }

    private void moveToInactive(BalancingDestination proxy) {
        activeProxies.remove(proxy);
        checkingProxies.remove(proxy);
        inactiveProxies.addLast(proxy);
    }

    private CompletableFuture<Void> ping(BalancingDestination client, ScheduledExecutorService executorService, Duration pingTimeout) {
        CompletableFuture<Void> f = client.createTransaction(pingTimeout).thenCompose(tx -> client.pingTransaction(tx))
                .thenAccept(unused -> {
                    lock.lock();
                    try {
                        if (activeProxies.add(client)) {
                            logger.info("proxy `{}` moved to active", client);
                        }
                    } finally {
                        lock.unlock();
                    }
                })
                .exceptionally(ex -> {
                    lock.lock();
                    try {
                        moveToInactive(client);
                    } finally {
                        lock.unlock();
                    }
                    logger.info("proxy `{}` ping transaction failed, moved to inactive: {}", client, ex);
                    return null;
                });

        executorService.schedule(
                () -> {
                    if (!f.isDone()) {
                        lock.lock();
                        try {
                            moveToInactive(client);
                        } finally {
                            lock.unlock();
                        }
                        logger.info("proxy `{}` ping timeout, moved to inactive", client);
                        f.cancel(true);
                    }
                },
                pingTimeout.toMillis(), TimeUnit.MILLISECONDS
        );

        return f;
    }

    public CompletableFuture<Void> ping(ScheduledExecutorService executorService, Duration pingTimeout) {
        lock.lock();
        try {
            while (checkingProxies.size() < numberOfProxiesToPing && inactiveProxies.size() > 0) {
                checkingProxies.add(inactiveProxies.removeFirst());
            }
            CompletableFuture<?>[] futures =
                    checkingProxies.stream().map(
                            proxy -> ping(proxy, executorService, pingTimeout)
                    ).toArray(CompletableFuture<?>[]::new);
            return CompletableFuture.allOf(futures);
        } finally {
            lock.unlock();
        }
    }
}
