package ru.yandex.yt.ytclient.rpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.rpcproxy.TReqPingTransaction;
import ru.yandex.yt.rpcproxy.TReqStartTransaction;
import ru.yandex.yt.rpcproxy.TRspPingTransaction;
import ru.yandex.yt.rpcproxy.TRspStartTransaction;
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

    private int aliveCount;
    final private Timer timer = new Timer();

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
            builder.body().setSticky(false);
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

    public BalancingRpcClient(Duration failoverTimeout, Duration pingTimeout, RpcClient ... destinations) {
        this.failoverTimeout = failoverTimeout;
        this.pingTimeout = pingTimeout;
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
        timer.cancel();
    }

    private CompletionStage<List<byte[]>> sendOnce(Destination dst, RpcClientRequest request) {
        CompletableFuture<List<byte[]>> f = new CompletableFuture<>();

        RpcClientResponseHandler handler = new RpcClientResponseHandler() {
            @Override
            public void onAcknowledgement() {
                if (request.isOneWay()) {
                    //logger.error("cancel `{}`", s.task);
                    f.complete(null);
                }
            }

            @Override
            public void onResponse(List<byte[]> attachments) {
                //logger.error("cancel `{}`", s.task);
                if (request.isOneWay()) {
                    f.completeExceptionally(new IllegalStateException("Server replied to a one-way request"));
                } else {
                    f.complete(attachments);
                }
            }

            @Override
            public void onError(Throwable error) {
                try {
                    f.completeExceptionally(error);
                } catch (Throwable e) {
                    // ignore
                }
            }
        };

        inflight.inc();
        total.inc();

        final RpcClientRequestControl control = dst.client.send(request, handler);

        return f.whenComplete((a, b) -> {
            if (b instanceof CancellationException) {
                // TODO: log here
                control.cancel();
            }

            inflight.dec();
        });
    }

    private static class DelayedFutureTask {
        CompletableFuture<List<byte[]>> f;
        TimerTask task;

        void cancel() {
            task.cancel();
            if (f != null) {
                f.cancel(true);
            }
        }
    }

    private CompletionStage<List<byte[]>> delayedExecute(List<CompletionStage<List<byte[]>>> prevSteps, long delay, Supplier<CompletionStage<List<byte[]>>> what) {
        CompletableFuture<List<byte[]>> result = new CompletableFuture<>();
        DelayedFutureTask task = new DelayedFutureTask();

        task.task = new TimerTask() {
            @Override
            public void run() {
                try {
                    boolean completeAny = false;
                    for (CompletionStage<List<byte[]>> step : prevSteps) {
                        CompletableFuture s = step.toCompletableFuture();
                        completeAny |= s.isDone() && !s.isCompletedExceptionally();
                    }

                    if (!completeAny) {
                        // run new future
                        if (delay > 0) {
                            failover.inc();
                        }

                        task.f = what.get().toCompletableFuture();
                        task.f.whenComplete((res, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                            } else {
                                result.complete(res);
                            }
                        });
                    }
                } catch (Throwable e) {
                    logger.error("timer error", e);
                    // System.exit(1);
                }
            }
        };

        timer.schedule(task.task, delay);

        return result.whenComplete((a, error) -> {
            if (error instanceof CancellationException) {
                //logger.error("cancel `{}`", task);
                task.cancel();
            }
        });
    }

    private List<Destination> selectDestinations() {
        final int maxSelect = 3;
        final ArrayList<Destination> result = new ArrayList<>();
        result.ensureCapacity(maxSelect);

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

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                f.cancel(true);
            }
        }, pingTimeout.toMillis());

        return f;
    }

    private void schedulePing() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                pingDeadDestinations();
            }
        }, 2*pingTimeout.toMillis());
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

        ImmutableList.Builder<CompletionStage<List<byte[]>>> builder = ImmutableList.builder();

        long delta = failoverTimeout.toMillis();
        int i = 0;

        for (Destination client : destinations) {
            long delay = delta * i;
            CompletionStage<List<byte[]>> currentStep;
            if (delay == 0) {
                currentStep = sendOnce(client, request);
            } else {
                currentStep = delayedExecute(builder.build(), delay, () -> sendOnce(client, request));
            }
            builder.add(currentStep);
            i ++;
        }

        CompletableFuture<List<byte[]>> result = new CompletableFuture<>();

        AtomicInteger count = new AtomicInteger(0);
        int totalCount = destinations.size();
        final ImmutableList<CompletionStage<List<byte[]>>> allSteps = builder.build();

        for (CompletionStage<List<byte[]>> step : allSteps) {
            step.whenComplete((value, error) -> {
                if (error != null) {
                    if (count.incrementAndGet() == totalCount) {
                        result.completeExceptionally(error);
                    }
                } else {
                    result.complete(value);
                    for (CompletionStage<List<byte[]>> stepLocal : allSteps) {
                        if (step != stepLocal) {
                            stepLocal.toCompletableFuture().cancel(true);
                        }
                    }
                }
            });
        }

        result.whenComplete((a, b) -> {
            if (b instanceof CancellationException) {
                for (CompletionStage<List<byte[]>> step : allSteps) {
                    step.toCompletableFuture().cancel(true);
                }
            }
        }).thenAccept(r -> {
            if (request.isOneWay()) {
                handler.onAcknowledgement();
            } else {
                handler.onResponse(r);
            }
        }).exceptionally(error -> {
            handler.onError(error);
            return null;
        });

        return () -> result.toCompletableFuture().cancel(true);
    }
}
