package tech.ytsaurus.client.rows;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;

@NonNullApi
@NonNullFields
public class UnversionedLookupRowsResult extends LookupRowsResult {
    private static final Logger logger = LoggerFactory.getLogger(UnversionedLookupRowsResult.class);

    private final TRowsetDescriptor rowsetDescriptor;
    private final List<byte[]> attachments;
    private final Executor heavyExecutor;

    private final SerializationResolver serializationResolver;

    private UnversionedLookupRowsResult(
            TRowsetDescriptor rowsetDescriptor,
            List<byte[]> attachments,
            List<Integer> unavailableKeyIndexes,
            Executor heavyExecutor,
            SerializationResolver serializationResolver
    ) {
        super(unavailableKeyIndexes);
        this.rowsetDescriptor = Objects.requireNonNull(rowsetDescriptor);
        this.attachments = Objects.requireNonNull(attachments);
        this.heavyExecutor = Objects.requireNonNull(heavyExecutor);
        this.serializationResolver = serializationResolver;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TRowsetDescriptor rowsetDescriptor;
        private List<byte[]> attachments;
        private List<Integer> unavailableKeyIndexes = List.of();
        private Executor heavyExecutor;
        private SerializationResolver serializationResolver;

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

        public UnversionedLookupRowsResult build() {
            return new UnversionedLookupRowsResult(rowsetDescriptor, attachments, unavailableKeyIndexes,
                    heavyExecutor, serializationResolver);
        }
    }

    public CompletableFuture<UnversionedRowset> getUnversionedRowset() {
        return handleResponseFromDescriptor(() -> ApiServiceUtil.deserializeUnversionedRowset(
                rowsetDescriptor, attachments));
    }

    public <T> CompletableFuture<List<T>> getRowsList(YTreeRowSerializer<T> serializer) {
        return handleResponseFromDescriptor(() -> {
            final ConsumerSourceRet<T> result = ConsumerSource.list();
            ApiServiceUtil.deserializeUnversionedRowset(rowsetDescriptor,
                    attachments, serializer, result, serializationResolver);
            return result.get();
        });
    }

    public <T> CompletableFuture<Void> handleWithConsumer(YTreeRowSerializer<T> serializer,
                                                          ConsumerSource<T> consumer) {
        return handleResponseFromDescriptor(() -> {
            ApiServiceUtil.deserializeUnversionedRowset(rowsetDescriptor,
                    attachments, serializer, consumer, serializationResolver);
            return null;
        });
    }

    private <T> CompletableFuture<T> handleResponseFromDescriptor(java.util.concurrent.Callable<T> fn) {
        return CompletableFuture.supplyAsync(() -> {
            logger.trace("LookupRows incoming rowset descriptor: {}", rowsetDescriptor);
            try {
                return fn.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, heavyExecutor);
    }
}


