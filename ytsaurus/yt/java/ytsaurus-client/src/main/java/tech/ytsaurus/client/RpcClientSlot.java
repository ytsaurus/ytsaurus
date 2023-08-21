package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.core.common.YTsaurusError;

class RpcClientSlot implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RpcClientSlot.class);

    private final AtomicBoolean seemsBroken;
    private final CompletableFuture<RpcClient> client;
    private HostPort address;

    RpcClientSlot() {
        this.seemsBroken = new AtomicBoolean(false);
        this.client = new CompletableFuture<>();
    }

    public Optional<RpcClient> getClient(Duration timeout) {
        try {
            return Optional.of(client.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get client from slot ", e);
        } catch (TimeoutException e) {
            return Optional.empty();
        }
    }

    public RpcClient getClient() {
        try {
            return client.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get client from slot ", e);
        }
    }

    public Optional<RpcClient> getOrCompleteClient() {
        client.completeExceptionally(new Exception("RpcClientSlot is closed"));
        if (client.isDone() && !client.isCompletedExceptionally()) {
            return Optional.of(getClient());
        }
        return Optional.empty();
    }

    @Override
    public void close() {
        Optional<RpcClient> maybeClient = getOrCompleteClient();
        maybeClient.ifPresent(this::closeClient);
    }

    CompletableFuture<RpcClient> getClientFuture() {
        return client;
    }

    void setClient(RpcClient client, HostPort address) {
        RpcClient failureDetectingClient = new FailureDetectingRpcClient(
                client,
                YTsaurusError::isUnrecoverable,
                (e) -> setSeemsBroken()
        );
        this.address = address;
        this.client.complete(failureDetectingClient);
    }

    boolean seemsBroken() {
        return seemsBroken.get();
    }

    private void setSeemsBroken() {
        seemsBroken.set(true);
        logger.info("Channel `{}` seems broken", client.getNow(null));
    }

    HostPort getAddress() {
        return address;
    }

    boolean isClientDone() {
        return client.isDone();
    }

    private void closeClient(RpcClient client) {
        try {
            client.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), "Error while terminating channel pool");
        }
    }
}
