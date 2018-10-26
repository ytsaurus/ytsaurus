package ru.yandex.yt.ytclient.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private final DataCenterMetricsHolder metricsHolder;
    private final BalancingDestinationMetricsHolder destMetricsHolder;
    private final RpcOptions options;

    private final String dc;
    private final ArrayList<BalancingDestination> backends;
    private int aliveCount;
    private final double weight;

    public DataCenter(String dc, BalancingDestination[] backends) {
        this(dc, backends, -1.0);
    }

    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            DataCenterMetricsHolder metricsHolder,
            BalancingDestinationMetricsHolder destMetricsHolder)
    {
        this(dc, backends, -1.0, metricsHolder, destMetricsHolder, new RpcOptions());
    }

    public DataCenter(String dc, BalancingDestination[] backends, double weight) {
        this(dc, backends, weight, new RpcOptions());
    }

    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            double weight,
            RpcOptions options)
    {
        this(dc, backends, weight, options.getDataCenterMetricsHolder(), options.getDestinationMetricsHolder(), options);
    }

    @Deprecated
    public DataCenter(
            String dc,
            BalancingDestination[] backends,
            double weight,
            DataCenterMetricsHolder metricsHolder,
            BalancingDestinationMetricsHolder destMetricsHolder,
            RpcOptions options)
    {
        this.dc = dc;
        this.backends = new ArrayList<>(Arrays.asList(backends));
        this.aliveCount = backends.length;
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

    private void setAlive(BalancingDestination dst) {
        synchronized (backends) {
            if (dst.getIndex() >= aliveCount) {
                swap(aliveCount, dst.getIndex());
                aliveCount++;
                logger.info("backend `{}` is alive", dst);
            }
        }
    }

    private void setDead(BalancingDestination dst, boolean remove, Throwable reason) {
        synchronized (backends) {
            if (dst.getIndex() < aliveCount) {
                aliveCount--;
                swap(aliveCount, dst.getIndex());
                logger.info("backend `{}` is dead, reason `{}`", dst, reason.toString());
                dst.resetTransaction();
            }
            if (remove) {
                int lastIdx = backends.size() - 1;
                swap(lastIdx, dst.getIndex());
                backends.remove(lastIdx);
            }
        }
    }

    public void addProxies(Set<RpcClient> proxies) {
        synchronized (backends) {
            backends.ensureCapacity(backends.size() + proxies.size());

            int index = backends.size();
            int prevSize = backends.size();
            for (RpcClient proxy : proxies) {
                backends.add(new BalancingDestination(dc, proxy, index ++, destMetricsHolder, options));
            }

            if (prevSize == aliveCount) {
                aliveCount = backends.size();
            }
        }
    }

    public void removeProxies(Set<RpcClient> proxies) {
        final ArrayList<BalancingDestination> removeList = new ArrayList<>();
        synchronized (backends) {
            removeList.ensureCapacity(proxies.size());
            for (BalancingDestination dest : backends) {
                if (proxies.contains(dest.getClient())) {
                    removeList.add(dest);
                }
            }
        }

        for (BalancingDestination removed: removeList) {
            setDead(removed, true, new Exception("proxy was removed from list"));
        }
    }

    public boolean isAlive() {
        return aliveCount > 0;
    }

    public void setDead(int index, Throwable reason) {
        setDead(backends.get(index), false, reason);
    }

    public void setAlive(int index) {
        setAlive(backends.get(index));
    }

    public void close() {
        synchronized (backends) {
            for (BalancingDestination client : backends) {
                client.close();
            }
        }
    }

    private void swap(int i, int j) {
        Collections.swap(backends, i, j);

        backends.get(i).setIndex(i);
        backends.get(j).setIndex(j);
    }

    public List<RpcClient> getAliveDestinations() {
        return getAliveDestinations(BalancingDestination::getClient);
    }

    public <T> List<T> getAliveDestinations(Function<BalancingDestination, T> func) {
        synchronized (backends) {
            return backends.subList(0, aliveCount).stream().map(func).collect(Collectors.toList());
        }
    }

    public List<RpcClient> selectDestinations(final int maxSelect, Random rnd) {
        return selectDestinations(maxSelect, rnd, BalancingDestination::getClient);
    }

    public <T> List<T> selectDestinations(final int maxSelect, Random rnd, Function<BalancingDestination, T> func) {
        final ArrayList<T> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

        synchronized (backends) {
            int count = aliveCount;

            while (count != 0 && result.size() < maxSelect) {
                int idx = rnd.nextInt(count);
                result.add(func.apply(backends.get(idx)));
                swap(idx, count-1);
                --count;
            }
        }

        return result;
    }

    private CompletableFuture<Void> ping(BalancingDestination client, ScheduledExecutorService executorService, Duration pingTimeout) {
        CompletableFuture<Void> f = client.createTransaction(pingTimeout).thenCompose(tx -> client.pingTransaction(tx))
            .thenAccept(unused -> setAlive(client))
            .exceptionally(ex -> {
                setDead(client, false, ex);
                return null;
            });

        executorService.schedule(
            () -> {
                if (!f.isDone()) {
                    setDead(client, false, new Exception("ping timeout"));
                    f.cancel(true);
                }
            },
            pingTimeout.toMillis(), TimeUnit.MILLISECONDS
        );

        return f;
    }

    public CompletableFuture<Void> ping(ScheduledExecutorService executorService, Duration pingTimeout) {
        synchronized (backends) {
            CompletableFuture<Void> futures[] = new CompletableFuture[backends.size()];
            for (int i = 0; i < backends.size(); ++i) {
                futures[i] = ping(backends.get(i), executorService, pingTimeout);
            }

            return CompletableFuture.allOf(futures);
        }
    }
}
