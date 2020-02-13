package ru.yandex.yt.ytclient.proxy.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcError;

public class RpcClientSlot implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RpcClientSlot.class);

    private final AtomicBoolean seemsBroken;
    private final CompletableFuture<RpcClient> client;
    private HostPort address;

    RpcClientSlot() {
        this.seemsBroken = new AtomicBoolean(false);
        this.client = new CompletableFuture<>();
    }

    public RpcClient getClient() {
        try {
            return client.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public void close() {
        client.completeExceptionally(new Exception("RpcClientSlot is closed"));
        if (client.isDone()) {
            if (!client.isCompletedExceptionally()) {
                closeClient(getClient());
            }
        }
    }

    CompletableFuture<RpcClient> getClientFuture() {
        return client;
    }

    void setClient(RpcClient client, HostPort address) {
        RpcClient failureDetectingClient = new FailureDetectingRpcClient(
                client,
                RpcError::isUnrecoverable,
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

    boolean isClientReady() {
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
