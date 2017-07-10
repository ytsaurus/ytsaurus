package ru.yandex.yt.ytclient.rpc.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;

/**
 * @author aozeritsky
 */
public final class DataCenter {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    private BalancingRpcClient balancingRpcClient;
    private final String dc;
    private final BalancingDestination[] backends;
    private int aliveCount;
    final private Random rnd = new Random();

    public DataCenter(BalancingRpcClient balancingRpcClient, String dc, BalancingDestination[] backends) {
        this.balancingRpcClient = balancingRpcClient;
        this.dc = dc;
        this.backends = backends;
        this.aliveCount = backends.length;
    }

    void setAlive(BalancingDestination dst) {
        synchronized (backends) {
            if (dst.getIndex() >= aliveCount) {
                BalancingDestination t = backends[aliveCount];
                backends[aliveCount] = backends[dst.getIndex()];
                backends[dst.getIndex()] = t;
                dst.setIndex(aliveCount);
                aliveCount++;

                logger.info("backend `{}` is alive", dst);
            }
        }
    }

    void setDead(BalancingDestination dst) {
        synchronized (backends) {
            if (dst.getIndex() < aliveCount) {
                aliveCount--;
                BalancingDestination t = backends[aliveCount];
                backends[aliveCount] = backends[dst.getIndex()];
                backends[dst.getIndex()] = t;
                dst.setIndex(aliveCount);

                logger.info("backend `{}` is dead", dst);
                dst.resetTransaction();
            }
        }
    }

    public void close() {
        for (BalancingDestination client : backends) {
            client.close();
        }
    }

    public List<BalancingDestination> selectDestinations(final int maxSelect) {
        final ArrayList<BalancingDestination> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

        rnd.ints(maxSelect);

        synchronized (backends) {
            int count = aliveCount;

            while (count != 0 && result.size() < maxSelect) {
                int idx = rnd.nextInt(count);
                BalancingDestination t = backends[idx];
                backends[idx] = backends[count - 1];
                backends[count - 1] = t;
                result.add(t);
                --count;
            }
        }

        return result;
    }

    private CompletableFuture<Void> ping(BalancingDestination client, ScheduledExecutorService executorService, Duration pingTimeout) {
        CompletableFuture<Void> f = client.createTransaction().thenCompose(id -> client.pingTransaction(id))
            .thenAccept(unused -> setAlive(client))
            .exceptionally(unused -> {
                setDead(client);
                return null;
            });

        executorService.schedule(
            () -> {
                if (!f.isDone()) {
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
