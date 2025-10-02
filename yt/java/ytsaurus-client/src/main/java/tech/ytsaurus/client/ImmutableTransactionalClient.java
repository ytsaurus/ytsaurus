package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.MultiLookupRowsRequest;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.LookupRowsResult;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.rows.YTreeRowSerializer;

public interface ImmutableTransactionalClient {
    CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?, ?> request);

    @Deprecated
    default CompletableFuture<UnversionedRowset> lookupRows(
            AbstractLookupRowsRequest.Builder<?, ?> request) {
        return lookupRows(request.build());
    }

    <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    );

    @Deprecated
    default <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest.Builder<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return lookupRows(request.build(), serializer);
    }

    CompletableFuture<List<UnversionedRowset>> multiLookupRows(MultiLookupRowsRequest request);

    <T> CompletableFuture<List<List<T>>> multiLookupRows(
            MultiLookupRowsRequest request,
            YTreeRowSerializer<T> serializer
    );

    <T> CompletableFuture<LookupRowsResult<List<T>>> lookupRowsWithPartialResult(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    );

    default <T> CompletableFuture<LookupRowsResult<List<T>>> lookupRowsWithPartialResult(
            AbstractLookupRowsRequest.Builder<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return lookupRowsWithPartialResult(request.build(), serializer);
    }

    CompletableFuture<LookupRowsResult<UnversionedRowset>> lookupRowsWithPartialResult(
            AbstractLookupRowsRequest<?, ?> request
    );

    default CompletableFuture<LookupRowsResult<UnversionedRowset>> lookupRowsWithPartialResult(
            AbstractLookupRowsRequest.Builder<?, ?> request
    ) {
        return lookupRowsWithPartialResult(request.build());
    }

    CompletableFuture<LookupRowsResult<VersionedRowset>> versionedLookupRowsWithPartialResult(
            AbstractLookupRowsRequest<?, ?> request
    );

    default CompletableFuture<LookupRowsResult<VersionedRowset>> versionedLookupRowsWithPartialResult(
            AbstractLookupRowsRequest.Builder<?, ?> request
    ) {
        return versionedLookupRowsWithPartialResult(request.build());
    }

    <T> CompletableFuture<List<LookupRowsResult<List<T>>>> multiLookupRowsWithPartialResult(
            MultiLookupRowsRequest request,
            YTreeRowSerializer<T> serializer
    );

    default <T> CompletableFuture<List<LookupRowsResult<List<T>>>> multiLookupRowsWithPartialResult(
            MultiLookupRowsRequest.Builder request,
            YTreeRowSerializer<T> serializer
    ) {
        return multiLookupRowsWithPartialResult(request.build(), serializer);
    }

    CompletableFuture<List<LookupRowsResult<UnversionedRowset>>> multiLookupRowsWithPartialResult(
            MultiLookupRowsRequest request
    );

    default CompletableFuture<List<LookupRowsResult<UnversionedRowset>>> multiLookupRowsWithPartialResult(
            MultiLookupRowsRequest.Builder request
    ) {
        return multiLookupRowsWithPartialResult(request.build());
    }

    CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?, ?> request);

    @Deprecated
    default CompletableFuture<VersionedRowset> versionedLookupRows(
            AbstractLookupRowsRequest.Builder<?, ?> request) {
        return versionedLookupRows(request.build());
    }

    CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request);

    <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeRowSerializer<T> serializer
    );

    <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer,
                                           ConsumerSource<T> consumer);

    CompletableFuture<SelectRowsResult> selectRowsV2(SelectRowsRequest request);


    default CompletableFuture<UnversionedRowset> selectRows(
            SelectRowsRequest.BuilderBase<?> request) {
        return selectRows(request.build());
    }

    default <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest.BuilderBase<?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return selectRows(request.build(), serializer);
    }

    default <T> CompletableFuture<Void> selectRows(
            SelectRowsRequest.BuilderBase<?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return selectRows(request.build(), serializer, consumer);
    }

    default CompletableFuture<SelectRowsResult> selectRowsV2(
            SelectRowsRequest.BuilderBase<?> request) {
        return selectRowsV2(request.build());
    }

    default CompletableFuture<UnversionedRowset> selectRows(String query) {
        return selectRows(query, null);
    }

    default CompletableFuture<UnversionedRowset> selectRows(String query, @Nullable Duration requestTimeout) {
        return selectRows(SelectRowsRequest.builder().setQuery(query).setTimeout(requestTimeout).build());
    }
}
