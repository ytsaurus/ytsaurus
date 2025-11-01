package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TRspLookupRows;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;

@NonNullApi
@NonNullFields
public class UnversionedLookupRowsResultV2 extends LookupRowsResult<UnversionedRowset> {
    private static final Logger logger = LoggerFactory.getLogger(UnversionedLookupRowsResultV2.class);

    private final RpcClientResponse<TRspLookupRows> response; // for single lookup
    private final TRowsetDescriptor rowsetDescriptor; // for multi subresponse
    private final List<byte[]> attachments; // for multi subresponse
    private final Executor heavyExecutor;
    private final SerializationResolver serializationResolver;

    UnversionedLookupRowsResultV2(
            RpcClientResponse<TRspLookupRows> response,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        super(response.body().getUnavailableKeyIndexesList());
        this.response = response;
        this.rowsetDescriptor = null;
        this.attachments = null;
        this.heavyExecutor = heavyExecutor;
        this.serializationResolver = serializationResolver;
    }

    UnversionedLookupRowsResultV2(
            TRowsetDescriptor rowsetDescriptor,
            List<byte[]> attachments,
            List<Integer> unavailableKeyIndexes,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        super(unavailableKeyIndexes);
        this.response = null;
        this.rowsetDescriptor = rowsetDescriptor;
        this.attachments = attachments;
        this.heavyExecutor = heavyExecutor;
        this.serializationResolver = serializationResolver;
    }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private RpcClientResponse<TRspLookupRows> response;
        private TRowsetDescriptor rowsetDescriptor;
        private List<byte[]> attachments;
        private List<Integer> unavailableKeyIndexes = java.util.List.of();
        private Executor heavyExecutor;
        private SerializationResolver serializationResolver;

        public Builder setResponse(RpcClientResponse<TRspLookupRows> response) {
            this.response = response;
            return this;
        }

        public Builder setRowsetDescriptor(TRowsetDescriptor rowsetDescriptor) {
            this.rowsetDescriptor = rowsetDescriptor;
            return this;
        }

        public Builder setAttachments(List<byte[]> attachments) {
            this.attachments = attachments;
            return this;
        }

        public Builder setUnavailableKeyIndexes(List<Integer> unavailableKeyIndexes) {
            this.unavailableKeyIndexes = unavailableKeyIndexes;
            return this;
        }

        public Builder setHeavyExecutor(Executor heavyExecutor) {
            this.heavyExecutor = heavyExecutor;
            return this;
        }

        public Builder setSerializationResolver(SerializationResolver serializationResolver) {
            this.serializationResolver = serializationResolver;
            return this;
        }

        public UnversionedLookupRowsResultV2 build() {
            if (response != null) {
                return new UnversionedLookupRowsResultV2(response, heavyExecutor, serializationResolver);
            }
            if (rowsetDescriptor == null || attachments == null) {
                throw new IllegalStateException("Either response or (rowsetDescriptor+attachments) must be set");
            }
            return new UnversionedLookupRowsResultV2(rowsetDescriptor, attachments, unavailableKeyIndexes,
                    heavyExecutor, serializationResolver);
        }
    }

    public CompletableFuture<UnversionedRowset> getUnversionedRowset() {
        if (response != null) {
            return handleResponse(rsp -> ApiServiceUtil.deserializeUnversionedRowset(
                    rsp.body().getRowsetDescriptor(), rsp.attachments()));
        }
        return handleResponseFromDescriptor(() -> ApiServiceUtil.deserializeUnversionedRowset(
                rowsetDescriptor, attachments));
    }

    public <T> CompletableFuture<List<T>> getRowsList(YTreeRowSerializer<T> serializer) {
        if (response != null) {
            return handleResponse(rsp -> {
                final ConsumerSourceRet<T> result = ConsumerSource.list();
                ApiServiceUtil.deserializeUnversionedRowset(rsp.body().getRowsetDescriptor(),
                        rsp.attachments(), serializer, result, serializationResolver);
                return result.get();
            });
        }
        return handleResponseFromDescriptor(() -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(rowsetDescriptor,
                    attachments, serializer, result, serializationResolver);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> handleWithConsumer(YTreeRowSerializer<T> serializer,
                                                          ConsumerSource<T> consumer) {
        if (response != null) {
            return handleResponse(rsp -> {
                ApiServiceUtil.deserializeUnversionedRowset(rsp.body().getRowsetDescriptor(),
                        rsp.attachments(), serializer, consumer, serializationResolver);
                return null;
            });
        }
        return handleResponseFromDescriptor(() -> {
            ApiServiceUtil.deserializeUnversionedRowset(rowsetDescriptor,
                    attachments, serializer, consumer, serializationResolver);
            return null;
        });
    }

    private <T> CompletableFuture<T> handleResponse(Function<RpcClientResponse<TRspLookupRows>, T> fn) {
        return RpcUtil.applyAsync(
                CompletableFuture.completedFuture(response),
                rsp -> {
                    logger.trace("LookupRows incoming rowset descriptor: {}", rsp.body().getRowsetDescriptor());
                    return fn.apply(rsp);
                },
                heavyExecutor);
    }

    private <T> CompletableFuture<T> handleResponseFromDescriptor(java.util.concurrent.Callable<T> fn) {
        return RpcUtil.applyAsync(
                CompletableFuture.completedFuture(Boolean.TRUE),
                ignored -> {
                    logger.trace("LookupRows incoming rowset descriptor: {}", rowsetDescriptor);
                    try {
                        return fn.call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                heavyExecutor);
    }
}


