package ru.yandex.yt.ytclient.proxy;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.misc.YtTimestamp;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public class ApiServiceTransaction implements AutoCloseable {
    private final ApiServiceClient client;
    private final YtGuid id;
    private final YtTimestamp startTimestamp;
    private final boolean ping;
    private final boolean sticky;
    private volatile boolean closed;

    public ApiServiceClient getClient() {
        return client;
    }

    public YtGuid getId() {
        return id;
    }

    public YtTimestamp getStartTimestamp() {
        return startTimestamp;
    }

    public boolean isPing() {
        return ping;
    }

    public boolean isSticky() {
        return sticky;
    }

    ApiServiceTransaction(ApiServiceClient client, YtGuid id, YtTimestamp startTimestamp, boolean ping,
            boolean sticky)
    {
        this.client = Objects.requireNonNull(client);
        this.id = Objects.requireNonNull(id);
        this.startTimestamp = Objects.requireNonNull(startTimestamp);
        this.ping = ping;
        this.sticky = sticky;
    }

    private CompletableFuture<Void> closeOnSuccess(CompletableFuture<Void> future) {
        future.thenAccept(ignored -> {
            closed = true;
        });
        return future;
    }

    public CompletableFuture<Void> ping() {
        if (closed) {
            throw new IllegalStateException("Transaction is closed");
        }
        return client.pingTransaction(id, sticky);
    }

    public CompletableFuture<Void> commit() {
        if (closed) {
            throw new IllegalStateException("Transaction is closed");
        }
        return closeOnSuccess(client.commitTransaction(id, sticky));
    }

    public CompletableFuture<Void> abort() {
        if (closed) {
            throw new IllegalStateException("Transaction is closed");
        }
        return closeOnSuccess(client.abortTransaction(id, sticky));
    }

    @Override
    public void close() {
        CompletableFuture<Void> future;
        try {
            future = abort();
        } catch (IllegalStateException ignored) {
            // транзакция уже закрыта
            return;
        }
        try {
            future.join();
        } catch (CancellationException | CompletionException ignored) {
            // игнорируем ошибки abort'а
        } finally {
            closed = true;
        }
    }

    public CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request) {
        return client.lookupRows(request, startTimestamp);
    }

    public CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request) {
        return client.versionedLookupRows(request, startTimestamp);
    }

    public CompletableFuture<Void> modifyRows(ModifyRowsRequest request) {
        return client.modifyRows(id, request);
    }
}
