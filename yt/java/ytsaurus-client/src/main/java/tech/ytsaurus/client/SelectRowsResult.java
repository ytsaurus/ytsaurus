package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.ConsumerSourceRet;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TRspSelectRows;


@NonNullApi
@NonNullFields
public class SelectRowsResult {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsResult.class);

    private final RpcClientResponse<TRspSelectRows> response;
    private final Executor heavyExecutor;

    private final SerializationResolver serializationResolver;

    public SelectRowsResult(
            RpcClientResponse<TRspSelectRows> response,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        this.response = response;
        this.heavyExecutor = heavyExecutor;
        this.serializationResolver = serializationResolver;
    }

    public CompletableFuture<UnversionedRowset> getUnversionedRowset() {
        return handleResponse(response ->
                ApiServiceUtil.deserializeUnversionedRowset(
                        response.body().getRowsetDescriptor(),
                        response.attachments()));
    }

    public <T> CompletableFuture<List<T>> getRowsList(YTreeRowSerializer<T> serializer) {
        return handleResponse(response -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, result, serializationResolver);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> handleWithConsumer(YTreeRowSerializer<T> serializer,
                                                          ConsumerSource<T> consumer) {
        return handleResponse(response -> {
            ApiServiceUtil.deserializeUnversionedRowset(response.body().getRowsetDescriptor(),
                    response.attachments(), serializer, consumer, serializationResolver);
            return null;
        });
    }

    public boolean isIncompleteOutput() {
        return response.body().getStatistics().getIncompleteOutput();
    }

    public boolean isIncompleteInput() {
        return response.body().getStatistics().getIncompleteInput();
    }

    private <T> CompletableFuture<T> handleResponse(Function<RpcClientResponse<TRspSelectRows>, T> fn) {
        return RpcUtil.applyAsync(
                CompletableFuture.completedFuture(response),
                response -> {
                    logger.trace("SelectRows incoming rowset descriptor: {}", response.body().getRowsetDescriptor());
                    return fn.apply(response);
                },
                heavyExecutor);
    }
}
