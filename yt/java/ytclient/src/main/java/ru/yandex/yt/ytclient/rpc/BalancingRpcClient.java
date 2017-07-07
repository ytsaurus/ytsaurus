package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.rpc.internal.BalancingDestination;
import ru.yandex.yt.ytclient.rpc.internal.BalancingResponseHandler;

import static java.lang.Integer.min;

/**
 * @author aozeritsky
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration failoverTimeout;
    final private Duration pingTimeout;
    final private Duration globalTimeout;
    final private String dataCenterName;
    final private DataCenter [] dataCenters;
    private DataCenter localDataCenter;
    final private Random rnd = new Random();
    final private ScheduledExecutorService executorService;
    final private RpcFailoverPolicy failoverPolicy;

    final private class DataCenter {
        private final String dc;
        private final BalancingDestination[] backends;
        private int aliveCount;

        DataCenter(String dc, BalancingDestination[] backends) {
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
                    aliveCount ++;

                    logger.info("backend `{}` is alive", dst);
                }
            }
        }

        void setDead(BalancingDestination dst) {
            synchronized (backends) {
                if (dst.getIndex() < aliveCount) {
                    aliveCount --;
                    BalancingDestination t = backends[aliveCount];
                    backends[aliveCount] = backends[dst.getIndex()];
                    backends[dst.getIndex()] = t;
                    dst.setIndex(aliveCount);

                    logger.info("backend `{}` is dead", dst);
                    dst.resetTransaction();
                }
            }
        }

        void close() {
            for (BalancingDestination client : backends) {
                client.close();
            }
        }

        List<BalancingDestination> selectDestinations(final int maxSelect) {
            final ArrayList<BalancingDestination> result = new ArrayList<>();
            result.ensureCapacity(maxSelect);

            rnd.ints(maxSelect);

            synchronized (backends) {
                int count = aliveCount;

                while (count != 0 && result.size() < maxSelect) {
                    int idx = rnd.nextInt(count);
                    BalancingDestination t = backends[idx];
                    backends[idx] = backends[count-1];
                    backends[count-1] = t;
                    result.add(t);
                    --count;
                }
            }

            return result;
        }

        private CompletableFuture<Void> ping(BalancingDestination client) {
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

        CompletableFuture<Void> ping() {
            synchronized (backends) {
                CompletableFuture<Void> futures[] = new CompletableFuture[backends.length];
                for (int i = 0; i < backends.length; ++i) {
                    futures[i] = ping(backends[i]);
                }

                return CompletableFuture.allOf(futures);
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

        this(
            failoverTimeout,
            globalTimeout,
            pingTimeout,
            connector,
            failoverPolicy,
            "unknown",
            ImmutableMap.of("unknown", Arrays.stream(destinations).collect(Collectors.toList()))
        );
    }

    public BalancingRpcClient(
        Duration failoverTimeout,
        Duration globalTimeout,
        Duration pingTimeout,
        BusConnector connector,
        RpcFailoverPolicy failoverPolicy,
        String dataCenter,
        Map<String, List<RpcClient>> dataCenters)
    {
        assert failoverTimeout.compareTo(globalTimeout) <= 0;

        this.dataCenterName = dataCenter;
        this.failoverPolicy = failoverPolicy;
        this.failoverTimeout = failoverTimeout;
        this.pingTimeout = pingTimeout;
        this.globalTimeout = globalTimeout;
        this.executorService = connector.executorService();
        this.dataCenters = new DataCenter[dataCenters.size()];
        this.localDataCenter = null;

        int i = 0;
        for (Map.Entry<String, List<RpcClient>> entity : dataCenters.entrySet()) {
            String dcName = entity.getKey();
            List<RpcClient> clients = entity.getValue();
            BalancingDestination [] destinations = new BalancingDestination[clients.size()];

            int j = 0;
            int index = 0;

            for (RpcClient client : clients) {
                index = j++;
                destinations[index] = new BalancingDestination(dcName, client, index);
            }
            DataCenter dc = new DataCenter(dcName, destinations);


            index = i++;
            this.dataCenters[index] = dc;
            if (dcName.equals(dataCenterName)) {
                this.localDataCenter = dc;
                this.dataCenters[index] = this.dataCenters[0];
                this.dataCenters[0] = this.localDataCenter;
            }
        }

        schedulePing();
    }

    @Override
    public void close() {
        for (DataCenter dc : dataCenters) {
            dc.close();
        }
    }

    private List<BalancingDestination> selectDestinations() {
        int maxSelect = 3;

        List<BalancingDestination> r = new ArrayList<>();
        for (DataCenter dc : dataCenters) {
            List<BalancingDestination> select = dc.selectDestinations(min(maxSelect, 2));
            maxSelect -= select.size();

            r.addAll(select);
        }

        synchronized (dataCenters) {
            // randomize
            int from = 0;
            if (localDataCenter != null) {
                from = 1;
            }

            for (int i = from; i < dataCenters.length; ++i) {
                int j = rnd.nextInt(dataCenters.length - i);
                DataCenter t = dataCenters[j];
                dataCenters[j] = dataCenters[i];
                dataCenters[i] = t;
            }
        }

        return r;
    }

    private void schedulePing() {
        executorService.schedule(
            () -> pingDataCenters(),
            2*pingTimeout.toMillis(),
            TimeUnit.MILLISECONDS);
    }

    private void pingDataCenters() {
        // logger.info("ping");

        CompletableFuture<Void> futures[] = new CompletableFuture[dataCenters.length];
        int i = 0;
        for (DataCenter entry : dataCenters) {
            futures[i++] = entry.ping();
        }

        CompletableFuture.allOf(futures).whenComplete((a, b) -> {
            schedulePing();
        });
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
