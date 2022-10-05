package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import ru.yandex.inside.yt.kosher.impl.ytree.object.YTreeRowSerializer;
import ru.yandex.yt.ytclient.object.ConsumerSource;
import ru.yandex.yt.ytclient.request.LookupRowsRequest;
import ru.yandex.yt.ytclient.request.MappedLookupRowsRequest;
import ru.yandex.yt.ytclient.request.SelectRowsRequest;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;
import ru.yandex.yt.ytclient.wire.VersionedRowset;

public interface ImmutableTransactionalClient {
    CompletableFuture<UnversionedRowset> lookupRows(LookupRowsRequest request);

    @Deprecated
    default CompletableFuture<UnversionedRowset> lookupRows(
            LookupRowsRequest.BuilderBase<?> request) {
        return lookupRows(request.build());
    }

    <T> CompletableFuture<List<T>> lookupRows(
            LookupRowsRequest request,
            YTreeRowSerializer<T> serializer
    );

    @Deprecated
    default <T> CompletableFuture<List<T>> lookupRows(
            LookupRowsRequest.BuilderBase<?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return lookupRows(request.build(), serializer);
    }

    CompletableFuture<VersionedRowset> versionedLookupRows(LookupRowsRequest request);

    @Deprecated
    default CompletableFuture<VersionedRowset> versionedLookupRows(
            LookupRowsRequest.BuilderBase<?> request) {
        return versionedLookupRows(request.build());
    }

    CompletableFuture<UnversionedRowset> lookupRows(MappedLookupRowsRequest<?> request);

    @Deprecated
    default <T> CompletableFuture<UnversionedRowset> lookupRows(
            MappedLookupRowsRequest.BuilderBase<?, ?> request) {
        return lookupRows(request.build());
    }

    <T> CompletableFuture<List<T>> lookupRows(
            MappedLookupRowsRequest<?> request,
            YTreeRowSerializer<T> serializer
    );

    @Deprecated
    default <T> CompletableFuture<List<T>> lookupRows(
            MappedLookupRowsRequest.BuilderBase<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return lookupRows(request.build(), serializer);
    }

    CompletableFuture<VersionedRowset> versionedLookupRows(MappedLookupRowsRequest<?> request);

    @Deprecated
    default <T> CompletableFuture<VersionedRowset> versionedLookupRows(
            MappedLookupRowsRequest.BuilderBase<?, ?> request) {
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
}
