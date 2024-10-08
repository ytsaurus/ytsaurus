package tech.ytsaurus.client.request;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import tech.ytsaurus.lang.NonNullApi;

/**
 * An interface for adding custom callbacks to api operations that can return the full result in the response.
 * */
@NonNullApi
public interface RequestMiddleware {
    /**
     * This method is calling when api operation is ready for processing.
     * For example, client can subscribe on future using {@link CompletableFuture#whenComplete(BiConsumer)}
     * */
    void onStarted(RequestBase<?, ?> request, CompletableFuture<?> response);
}
