package ru.yandex.yt.ytclient.rpc.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolderImpl;

/**
 * @author aozeritsky
 */
public final class DataCenter {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    private final DataCenterMetricsHolder metricsHolder;

    private final String dc;
    private final BalancingDestination[] backends;
    private int aliveCount;
    private final double weight;

    public DataCenter(String dc, BalancingDestination[] backends) {
        this(dc, backends, -1.0);
    }

    public DataCenter(String dc, BalancingDestination[] backends, DataCenterMetricsHolder metricsHolder) {
        this(dc, backends, -1.0, metricsHolder);
    }

    public DataCenter(String dc, BalancingDestination[] backends, double weight) {
        this(dc, backends, weight, DataCenterMetricsHolderImpl.instance);
    }

    public DataCenter(String dc, BalancingDestination[] backends, double weight, DataCenterMetricsHolder metricsHolder) {
        this.dc = dc;
        this.backends = backends;
        this.aliveCount = backends.length;
        this.weight = weight;
        this.metricsHolder = metricsHolder;
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

    private void setDead(BalancingDestination dst, Throwable reason) {
        synchronized (backends) {
            if (dst.getIndex() < aliveCount) {
                aliveCount--;
                swap(aliveCount, dst.getIndex());
                logger.info("backend `{}` is dead, reason `{}`", dst, reason.toString());
                dst.resetTransaction();
            }
        }
    }

    public boolean isAlive() {
        return aliveCount > 0;
    }

    public void setDead(int index, Throwable reason) {
        setDead(backends[index], reason);
    }

    public void setAlive(int index) {
        setAlive(backends[index]);
    }

    public void close() {
        for (BalancingDestination client : backends) {
            client.close();
        }
    }

    private void swap(int i, int j) {
        BalancingDestination t = backends[i];
        backends[i] = backends[j];
        backends[j] = t;

        backends[i].setIndex(i);
        backends[j].setIndex(j);
    }

    public List<BalancingDestination> getAliveDestinations() {
        synchronized (backends) {
            return Arrays.stream(backends, 0, aliveCount).collect(Collectors.toList());
        }
    }

    public List<BalancingDestination> selectDestinations(final int maxSelect, Random rnd) {
        final ArrayList<BalancingDestination> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

        synchronized (backends) {
            int count = aliveCount;

            while (count != 0 && result.size() < maxSelect) {
                int idx = rnd.nextInt(count);
                result.add(backends[idx]);
                swap(idx, count-1);
                --count;
            }
        }

        return result;
    }

    private CompletableFuture<Void> ping(BalancingDestination client, ScheduledExecutorService executorService, Duration pingTimeout) {
        CompletableFuture<Void> f = client.createTransaction(pingTimeout).thenCompose(id -> client.pingTransaction(id))
            .thenAccept(unused -> setAlive(client))
            .exceptionally(ex -> {
                setDead(client, ex);
                return null;
            });

        executorService.schedule(
            () -> {
                if (!f.isDone()) {
                    setDead(client, new Exception("ping timeout"));
                    f.cancel(true);
                }
            },
            pingTimeout.toMillis(), TimeUnit.MILLISECONDS
        );

        return f;
    }

    public CompletableFuture<Void> ping(ScheduledExecutorService executorService, Duration pingTimeout) {
        synchronized (backends) {
            CompletableFuture<Void> futures[] = new CompletableFuture[backends.length];
            for (int i = 0; i < backends.length; ++i) {
                futures[i] = ping(backends[i], executorService, pingTimeout);
            }

            return CompletableFuture.allOf(futures);
        }
    }
}
