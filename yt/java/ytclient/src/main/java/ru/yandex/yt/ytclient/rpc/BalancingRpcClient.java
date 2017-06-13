package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.proxy.ApiService;

/**
 * Created by aozeritsky on 24.05.2017.
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration failoverTimeout;
    final private Duration pingTimeout;
    final private Destination[] destinations;
    final private Random rnd = new Random();
    final private ScheduledExecutorService executorService;

    private int aliveCount;

    // TODO: move somewhere to core
    private static final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("ytclient");
    private static final Counter inflight = metrics.counter(MetricRegistry.name(BalancingRpcClient.class, "requests", "inflight"));
    private static final Counter failover = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "failover"));
    private static final Counter total = metrics.counter(MetricRegistry.name(BalancingRpcClient.class,"requests", "total"));

    final private class Destination {
        final RpcClient client;
        boolean isAlive;
        int index;

        final ApiService service;
        YtGuid transaction = null;

        Destination(RpcClient client, int index) {
            this.client = Objects.requireNonNull(client);
            isAlive = true;
            this.index = index;
            service = client.getService(ApiService.class);
        }

        void close() {
            client.close();
        }

        void setAlive() {
            synchronized (destinations) {
                if (index >= aliveCount) {
                    Destination t = destinations[aliveCount];
                    destinations[aliveCount] = destinations[index];
                    destinations[index] = t;
                    index = aliveCount;
                    aliveCount ++;

                    logger.info("backend `{}` is alive", client);
                }
            }
        }

        void setDead() {
            synchronized (destinations) {
                if (index < aliveCount) {
                    aliveCount --;
                    Destination t = destinations[aliveCount];
                    destinations[aliveCount] = destinations[index];
                    destinations[index] = t;
                    index = aliveCount;

                    logger.info("backend `{}` is dead", client);
                    transaction = null;
                }
            }
        }

        CompletableFuture<YtGuid> createTransaction() {
            if (transaction == null) {
                RpcClientRequestBuilder<TReqStartTransaction.Builder, RpcClientResponse<TRspStartTransaction>> builder =
                    service.startTransaction();
                builder.body().setType(ETransactionType.TABLET);
                builder.body().setSticky(true);
                return RpcUtil.apply(builder.invoke(), response -> {
                    YtGuid id = YtGuid.fromProto(response.body().getId());
                    return id;
                });
            } else {
                return CompletableFuture.completedFuture(transaction);
            }
        }

        CompletableFuture<Void> pingTransaction(YtGuid id) {
            RpcClientRequestBuilder<TReqPingTransaction.Builder, RpcClientResponse<TRspPingTransaction>> builder =
                service.pingTransaction();
            builder.body().setTransactionId(id.toProto());
            builder.body().setSticky(true);
            return RpcUtil.apply(builder.invoke(), response -> null).thenAccept(unused -> {
               transaction = id;
            });
        }

        CompletableFuture<Void> ping() {
            return createTransaction().thenCompose(id -> pingTransaction(id))
                .thenAccept(unused -> setAlive())
                .exceptionally(unused -> {
                    setDead();
                    return null;
                });
        }
    }

    public BalancingRpcClient(Duration failoverTimeout, Duration pingTimeout, BusConnector connector, RpcClient ... destinations) {
        this.failoverTimeout = failoverTimeout;
        this.pingTimeout = pingTimeout;
        this.executorService = connector.executorService();
        this.destinations = new Destination[destinations.length];
        int i = 0;
        for (RpcClient client : destinations) {
            int index = i++;
            this.destinations[index] = new Destination(client, index);
        }
        this.aliveCount = this.destinations.length;

        schedulePing();
    }

    @Override
    public void close() {
        for (Destination client : destinations) {
            client.close();
        }
    }

    private static class ResponseHandler implements RpcClientResponseHandler {
        private final CompletableFuture<List<byte[]>> f;
        private List<Destination> clients;
        private final RpcClientRequest request;
        private final boolean isOneWay;
        private int step;
        private final BalancingRpcClient parent;
        private final List<RpcClientRequestControl> cancelation;

        ResponseHandler(BalancingRpcClient parent, CompletableFuture<List<byte[]>> f, RpcClientRequest request, List<Destination> clients) {
            this.f = f;
            this.request = request;
            this.isOneWay = request.isOneWay();
            this.clients = clients;
            this.parent = parent;
            cancelation = new ArrayList<>();
            step = 0;
            send();
        }

        private void send() {
            RpcClient client;
            Destination dst = clients.get(0);
            clients = clients.subList(1, clients.size());
            client = dst.client;

            if (step > 0) {
                failover.inc();
            }

            step ++;

            inflight.inc();
            total.inc();

            cancelation.add(client.send(request, this));
            parent.executorService.schedule(() ->
                onTimeout()
            , step * parent.failoverTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void onTimeout() {
            synchronized (f) {
                if (!f.isDone()) {
                    if (!clients.isEmpty()){
                        send();
                    } else {
                        // global timeout
                    }
                }
            }
        }

        void cancel() {
            synchronized (f) {
                for (RpcClientRequestControl control : cancelation) {
                    inflight.dec();
                    control.cancel();
                }
            }
        }

        @Override
        public void onAcknowledgement() {
            if (isOneWay) {
                synchronized (f) {
                    if (!f.isDone()) {
                        f.complete(null);
                    }
                }
            }
        }

        @Override
        public void onResponse(List<byte[]> attachments) {
            synchronized (f) {
                if (!f.isDone()) {
                    if (isOneWay) {
                        // FATAL error, cannot recover
                        f.completeExceptionally(new IllegalStateException("Server replied to a one-way request"));
                    } else {
                        f.complete(attachments);
                    }
                }
            }
        }

        @Override
        public void onError(Throwable error) {
            synchronized (f) {
                if (!f.isDone()) {
                    // maybe use other proxy here?
                    f.completeExceptionally(error);
                }
            }
        }
    }

    private List<Destination> selectDestinations() {
        final int maxSelect = 3;
        final ArrayList<Destination> result = new ArrayList<>();
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
                Destination t = destinations[idx];
                destinations[idx] = destinations[count-1];
                destinations[count-1] = t;
                result.add(t);
                --count;
            }
        }

        return result;
    }

    private CompletableFuture<Void> pingDestination(Destination client) {
        CompletableFuture<Void> f = client.ping();

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
        List<Destination> destinations = selectDestinations();

        CompletableFuture<List<byte[]>> f = new CompletableFuture<>();

        ResponseHandler h = new ResponseHandler(this, f, request, destinations);

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
