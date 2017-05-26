package ru.yandex.yt.ytclient.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Created by aozeritsky on 24.05.2017.
 */
public class BalancingRpcClient implements RpcClient {
    final private Duration maxDelay;
    final private ArrayList<RpcClient> destinations;
    final private Timer timer = new Timer();

    public BalancingRpcClient(Duration maxDelay, RpcClient ... destinations) {
        this.maxDelay = maxDelay;

        this.destinations = new ArrayList<>();
        this.destinations.ensureCapacity(destinations.length);
        Collections.addAll(this.destinations, destinations);
    }

    @Override
    public void close() {
        for (RpcClient client : destinations) {
            client.close();
        }
    }

    public static <T> CompletionStage<T> exceptionallyCompose(
        CompletionStage<T> stage,
        Function<Throwable, ? extends CompletionStage<T>> fn) {
        return dereference(wrap(stage).exceptionally(fn));
    }

    public static <T> CompletionStage<T> dereference(
        CompletionStage<? extends CompletionStage<T>> stage) {
        return stage.thenCompose(Function.identity());
    }

    private static <T> CompletionStage<CompletionStage<T>> wrap(CompletionStage<T> future) {
        //noinspection unchecked
        return future.thenApply(CompletableFuture::completedFuture);
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
                    s.task.cancel();
                    f.complete(null);
                }
            }

            @Override
            public void onResponse(List<byte[]> attachments) {
                s.task.cancel();
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

    private final RuntimeException identityError = new RuntimeException("empty destinations");

    private CompletionStage<List<byte[]>> identity() {
        CompletableFuture<List<byte[]>> id = new CompletableFuture<>();
        id.completeExceptionally(identityError);
        return id;
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        Collections.shuffle(destinations);

        destinations.stream();

        CompletionStage<List<byte[]>> result = destinations.stream().reduce(identity(),
            (a, b) -> exceptionallyCompose(a, error -> {
                if (error != identityError && error.getCause() != identityError) {
                    // TODO: log here
                }
                return sendOnce(b, request);
            }),
            (a, b) -> exceptionallyCompose(a, error -> b)
        );

        result.thenAccept(r -> {
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
