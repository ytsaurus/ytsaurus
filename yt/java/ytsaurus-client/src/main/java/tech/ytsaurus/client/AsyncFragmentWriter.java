package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.request.WriteFragmentResult;

public interface AsyncFragmentWriter<T> extends AsyncWriter<T> {

    /**
     * Finish distributed write fragment process.
     * @return A CompletableFuture of WriteFragmentResult that must be used in FinishDistributedWriteSession
     */
    @Override
    CompletableFuture<WriteFragmentResult> finish();
}

