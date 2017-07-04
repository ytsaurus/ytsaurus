package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.rpc.internal.BalancingDestination;
import ru.yandex.yt.ytclient.rpc.internal.BalancingResponseHandler;

/**
 * @author aozeritsky
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration failoverTimeout;
    final private Duration pingTimeout;
    final private Duration globalTimeout;
    final private BalancingDestination[] destinations;
    final private Random rnd = new Random();
    final private ScheduledExecutorService executorService;
    final private RpcFailoverPolicy failoverPolicy;

    private int aliveCount;

    private void setAlive(BalancingDestination dst) {
        synchronized (destinations) {
            if (dst.getIndex() >= aliveCount) {
                BalancingDestination t = destinations[aliveCount];
                destinations[aliveCount] = destinations[dst.getIndex()];
                destinations[dst.getIndex()] = t;
                dst.setIndex(aliveCount);
                aliveCount ++;

                logger.info("backend `{}` is alive", dst);
            }
        }
    }

    private void setDead(BalancingDestination dst) {
        synchronized (destinations) {
            if (dst.getIndex() < aliveCount) {
                aliveCount --;
                BalancingDestination t = destinations[aliveCount];
                destinations[aliveCount] = destinations[dst.getIndex()];
                destinations[dst.getIndex()] = t;
                dst.setIndex(aliveCount);

                logger.info("backend `{}` is dead", dst);
                dst.resetTransaction();
            }
        }
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcClient ... destinations) {

        this(failoverTimeout, globalTimeout, pingTimeout, connector, new DefaultRpcFailoverPolicy(), destinations);
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcFailoverPolicy failoverPolicy,
        RpcClient ... destinations) {

        assert failoverTimeout.compareTo(globalTimeout) <= 0;

        this.failoverPolicy = failoverPolicy;
        this.failoverTimeout = failoverTimeout;
        this.pingTimeout = pingTimeout;
        this.globalTimeout = globalTimeout;
        this.executorService = connector.executorService();
        this.destinations = new BalancingDestination[destinations.length];
        int i = 0;
        for (RpcClient client : destinations) {
            int index = i++;
            this.destinations[index] = new BalancingDestination(client, index);
        }
        this.aliveCount = this.destinations.length;

        schedulePing();
    }

    @Override
    public void close() {
        for (BalancingDestination client : destinations) {
            client.close();
        }
    }

    private List<BalancingDestination> selectDestinations() {
        final int maxSelect = 3;
        final ArrayList<BalancingDestination> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

        rnd.ints(3);
        synchronized (destinations) {
            int count = aliveCount;
            if (count == 0) {
                // try some of dead clients
                count = destinations.length;
            }

            while (count != 0 && result.size() < maxSelect) {
                int idx = rnd.nextInt(count);
                BalancingDestination t = destinations[idx];
                destinations[idx] = destinations[count-1];
                destinations[count-1] = t;
                result.add(t);
                --count;
            }
        }

        return result;
    }

    private CompletableFuture<Void> pingDestination(BalancingDestination client) {
        CompletableFuture<Void> f = client.createTransaction().thenCompose(id -> client.pingTransaction(id))
            .thenAccept(unused -> setAlive(client))
            .exceptionally(unused -> {
                setDead(client);
                return null;
            });

        executorService.schedule(
            () -> {
                if (!f.isDone()) { f.cancel(true); }
            },
            pingTimeout.toMillis(), TimeUnit.MILLISECONDS
        );

        return f;
    }

    private void schedulePing() {
        executorService.schedule(
            () -> pingDeadDestinations(),
            2*pingTimeout.toMillis(),
            TimeUnit.MILLISECONDS);
    }

    private void pingDeadDestinations() {
        // logger.info("ping");

        synchronized (destinations) {
            CompletableFuture<Void> futures[] = new CompletableFuture[destinations.length];
            for (int i = 0; i < destinations.length; ++i) {
                futures[i] = pingDestination(destinations[i]);
            }

            CompletableFuture.allOf(futures).whenComplete((a, b) -> {
                schedulePing();
            });
        }
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        List<BalancingDestination> destinations = selectDestinations();

        CompletableFuture<List<byte[]>> f = new CompletableFuture<>();

        BalancingResponseHandler h = new BalancingResponseHandler(
            executorService,
            failoverPolicy,
            globalTimeout,
            failoverTimeout,
            f,
            request,
            destinations);

        f.whenComplete((result, error) -> {
            h.cancel();
            if (error == null) {
                if (request.isOneWay()) {
                    handler.onAcknowledgement();
                } else {
                    handler.onResponse(result);
                }
            } else {
                handler.onError(error);
            }
        });

        return () -> f.cancel(true);
    }
}
