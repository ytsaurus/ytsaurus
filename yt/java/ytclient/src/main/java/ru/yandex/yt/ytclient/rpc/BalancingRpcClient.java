package ru.yandex.yt.ytclient.rpc;

import java.util.Random;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import ru.yandex.yt.rpcproxy.TReqGetNode;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.ytclient.ytree.YTreeNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.Counter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.collect.ImmutableList;

/**
 * Created by aozeritsky on 24.05.2017.
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration maxDelay;
    final private Destination[] destinations;
    final private Random rnd = new Random();

    private int aliveCount;
    final private Timer timer = new Timer();

    // TODO: move somewhere to core
    final MetricRegistry metrics = SharedMetricRegistries.getOrCreate("default");
    final Counter inflight = metrics.counter("inflight");
    final Counter failover = metrics.counter("failover");
    final Counter total = metrics.counter("total");

    final private class Destination {
        final RpcClient client;
        boolean isAlive;
        int index;

        Destination(RpcClient client, int index) {
            this.client = client;
            isAlive = true;
            this.index = index;
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
                }
            }
        }
    }

    public BalancingRpcClient(Duration maxDelay, RpcClient ... destinations) {
        this.maxDelay = maxDelay;
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

    private static class SendData {
        RpcClientRequestControl control;
        TimerTask task;
    }

    private CompletionStage<List<byte[]>> sendOnce(Destination dst, RpcClientRequest request) {
        CompletableFuture<List<byte[]>> f = new CompletableFuture<>();

        SendData s = new SendData();

        RpcClientResponseHandler handler = new RpcClientResponseHandler() {
            @Override
            public void onAcknowledgement() {
                if (request.isOneWay()) {
                    //logger.error("cancel `{}`", s.task);
                    s.task.cancel();
                    f.complete(null);
                }
            }

            @Override
            public void onResponse(List<byte[]> attachments) {
                s.task.cancel();
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

        s.control = dst.client.send(request, handler);
        s.task = new TimerTask() {
            @Override
            public void run() {
                if (!f.isDone()) {
                    // TODO: log here
                    s.control.cancel();
                    f.cancel(true);
                    // mark destination as bad
                    dst.setDead();
                }
            }
        };

        timer.schedule(s.task, maxDelay.toMillis());

        return f.whenComplete((a, b) -> {
            if (b instanceof CancellationException) {
                // TODO: log here
                s.control.cancel();
            } else if (b != null) {
                dst.setDead();
            } else {
                dst.setAlive();
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
            }
        }

        return result;
    }

    public interface PingService {
        RpcClientRequestBuilder<TReqGetNode.Builder, RpcClientResponse<TRspGetNode>> getNode();
    }

    private CompletableFuture<YTreeNode> pingDestination(Destination client) {
        PingService service = client.client.getService(PingService.class);
        RpcClientRequestBuilder<TReqGetNode.Builder, RpcClientResponse<TRspGetNode>> builder = service.getNode();
        builder.body().setPath("//tmp");
        CompletableFuture<YTreeNode> f = RpcUtil
            .apply(builder.invoke(), response -> YTreeNode.parseByteString(response.body().getData()))
            .thenApply(node -> {
                client.setAlive();
                return node;
            }).exceptionally(unused -> null); // ignore exceptions ?

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                f.cancel(true);
            }
        }, maxDelay.toMillis());

        return f;
    }

    private void schedulePing() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                pingDeadDestinations();
            }
        }, 2*maxDelay.toMillis());
    }

    private void pingDeadDestinations() {
        // logger.info("ping");

        synchronized (destinations) {
            CompletableFuture<YTreeNode> futures[] = new CompletableFuture[destinations.length - aliveCount];
            for (int i = aliveCount; i < destinations.length; ++i) {
                futures[i - aliveCount] = pingDestination(destinations[i]);
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

        long delta = maxDelay.toMillis();
        int i = 0;

        for (Destination client : destinations) {
            long delay = delta * i;
            CompletionStage<List<byte[]>> currentStep = delayedExecute(builder.build(), delay, () -> sendOnce(client, request));
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
