package ru.yandex.yt.ytclient.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Created by aozeritsky on 24.05.2017.
 */
public class BalancingRpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BalancingRpcClient.class);

    final private Duration maxDelay;
    final private ArrayList<RpcClient> destinations;
    final private Timer timer = new Timer();

    public BalancingRpcClient(Duration maxDelay, RpcClient ... destinations) {
        this.maxDelay = maxDelay;
        this.destinations = new ArrayList<>();
        Collections.addAll(this.destinations, destinations);
    }

    public BalancingRpcClient(Duration maxDelay, List<RpcClient> destinations) {
        this.maxDelay = maxDelay;
        this.destinations = new ArrayList<>();
        this.destinations.addAll(destinations);
    }

    @Override
    public void close() {
        for (RpcClient client : destinations) {
            client.close();
        }
        timer.cancel();
    }

    private static class SendData {
        RpcClientRequestControl control;
        TimerTask task;
    }

    private CompletionStage<List<byte[]>> sendOnce(RpcClient dst, RpcClientRequest request) {
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
                f.completeExceptionally(error);
            }
        };

        s.control = dst.send(request, handler);
        s.task = new TimerTask() {
            @Override
            public void run() {
                if (!f.isDone()) {
                    // TODO: log here
                    s.control.cancel();
                    f.cancel(true);
                }
            }
        };

        timer.schedule(s.task, maxDelay.toMillis());

        return f.whenComplete((a, b) -> {
            if (b != null && b instanceof CancellationException) {
                // TODO: log here
                s.control.cancel();
            }
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
                boolean completeAny = false;
                for (CompletionStage<List<byte[]>> step : prevSteps) {
                    CompletableFuture s = step.toCompletableFuture();
                    completeAny |= s.isDone() && ! s.isCompletedExceptionally();
                }

                if (!completeAny) {
                    // run new future
                    task.f = what.get().toCompletableFuture();
                    task.f.whenComplete((res, error) -> {
                        if (error != null) {
                            result.completeExceptionally(error);
                        } else {
                            result.complete(res);
                        }
                    });
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

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        List<RpcClient> destinations = (List<RpcClient>)this.destinations.clone();
        Collections.shuffle(destinations);

        ArrayList<CompletionStage<List<byte[]>>> allSteps = new ArrayList<>();
        allSteps.ensureCapacity(destinations.size());

        long delta = maxDelay.toMillis();
        int i = 0;

        for (RpcClient client : destinations) {
            long delay = delta * i;
            List<CompletionStage<List<byte[]>>> currentList = (List<CompletionStage<List<byte[]>>>)allSteps.clone();
            CompletionStage<List<byte[]>> currentStep = delayedExecute(currentList, delay, () -> sendOnce(client, request));
            allSteps.add(currentStep);
            i ++;
        }

        CompletableFuture<List<byte[]>> result = new CompletableFuture<>();

        AtomicInteger count = new AtomicInteger(0);
        int totalCount = destinations.size();

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
